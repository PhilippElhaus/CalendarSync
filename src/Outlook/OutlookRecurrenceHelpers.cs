using System.Runtime.InteropServices;
using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync.src;

public partial class CalendarSyncService
{
	private Outlook.RecurrencePattern? TryGetRecurrencePattern(Outlook.AppointmentItem appt)
	{
		try
		{
			return appt.GetRecurrencePattern();
		}
		catch (COMException ex)
		{
			_logger.LogDebug(ex, "Failed to get recurrence pattern.");
			return null;
		}
	}

	private RecurrencePattern? BuildRecurrenceRule(Outlook.RecurrencePattern pattern, string? subject)
	{
		var freq = pattern.RecurrenceType switch
		{
			Outlook.OlRecurrenceType.olRecursDaily => FrequencyType.Daily,
			Outlook.OlRecurrenceType.olRecursWeekly => FrequencyType.Weekly,
			Outlook.OlRecurrenceType.olRecursMonthly => FrequencyType.Monthly,
			Outlook.OlRecurrenceType.olRecursYearly => FrequencyType.Yearly,
			_ => FrequencyType.None
		};

		if (freq == FrequencyType.None)
		{
			_logger.LogWarning("Unsupported recurrence type for event '{Subject}'. Skipping.", subject);
			return null;
		}

		var rule = new RecurrencePattern
		{
			Frequency = freq,
			Interval = pattern.Interval
		};

		if (freq == FrequencyType.Weekly)
		{
			ConfigureWeeklyRule(rule, pattern.DayOfWeekMask);
		}

		ApplyPatternEnd(rule, pattern);
		return rule;
	}

	private void ConfigureWeeklyRule(RecurrencePattern rule, Outlook.OlDaysOfWeek mask)
	{
		var byDay = new List<WeekDay>();
		if ((mask & Outlook.OlDaysOfWeek.olMonday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Monday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olTuesday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Tuesday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olWednesday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Wednesday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olThursday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Thursday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olFriday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Friday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olSaturday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Saturday));
		}
		if ((mask & Outlook.OlDaysOfWeek.olSunday) != 0)
		{
			byDay.Add(new WeekDay(DayOfWeek.Sunday));
		}
		rule.ByDay = byDay;
	}

	private void ApplyPatternEnd(RecurrencePattern rule, Outlook.RecurrencePattern pattern)
	{
		if (pattern.NoEndDate)
		{
			return;
		}

		if (pattern.Occurrences > 0)
		{
			rule.Count = pattern.Occurrences;
		}
		else if (pattern.PatternEndDate != DateTime.MinValue)
		{
			rule.Until = new CalDateTime(pattern.PatternEndDate.ToUniversalTime());
		}
	}

	private (Outlook.AppointmentItem seriesItem, DateTime? masterStart, DateTime? masterEnd, bool apptIsMaster) ResolveSeriesContext(
	Outlook.AppointmentItem appt,
	Outlook.RecurrencePattern pattern,
	DateTime baseStartLocal,
	DateTime baseEndLocal)
	{
		var apptIsMaster = false;
		try
		{
			apptIsMaster = appt.RecurrenceState == Outlook.OlRecurrenceState.olApptMaster;
		}
		catch (COMException)
		{
		}

		Outlook.AppointmentItem seriesItem = appt;
		Outlook.AppointmentItem? master = null;
		var releaseMaster = false;
		DateTime? masterStart = null;
		DateTime? masterEnd = null;

		if (!apptIsMaster)
		{
			try
			{
				master = pattern.Parent as Outlook.AppointmentItem;
				if (master != null && !ReferenceEquals(master, appt))
				{
					releaseMaster = true;
					try
					{
						var (resolvedStart, _) = NormalizeOutlookTimes(master.Start, master.StartUTC, $"master '{appt.Subject}' start");
						var (resolvedEnd, _) = NormalizeOutlookTimes(master.End, master.EndUTC, $"master '{appt.Subject}' end");
						masterStart = resolvedStart;
						masterEnd = resolvedEnd;
					}
					catch (COMException)
					{
						masterStart = null;
						masterEnd = null;
					}
					seriesItem = master;
				}
				else if (master != null)
				{
					masterStart = baseStartLocal;
					masterEnd = baseEndLocal;
				}
			}
			catch (COMException)
			{
			}
			finally
			{
				if (releaseMaster && master != null)
				{
					try
					{
						Marshal.ReleaseComObject(master);
					}
					catch
					{
					}
				}
			}
		}

		return (seriesItem, masterStart, masterEnd, apptIsMaster);
	}

	private (CalendarEvent calEvent, TimeSpan baseDuration) CreateRecurrenceCalendarEvent(
	RecurrencePattern rule,
	DateTime baseStartLocal,
	DateTime baseStartUtc,
	DateTime baseEndUtc,
	DateTime? masterStart,
	DateTime? masterEnd,
	Outlook.RecurrencePattern pattern,
	Outlook.AppointmentItem appt,
	bool apptIsMaster)
	{
		DateTime? patternSeriesStart = null;
		try
		{
			if (pattern.PatternStartDate != DateTime.MinValue)
			{
				var startDate = pattern.PatternStartDate.Date;
				var timeOfDay = pattern.StartTime != DateTime.MinValue
				? pattern.StartTime.TimeOfDay
				: (masterStart ?? baseStartLocal).TimeOfDay;
				patternSeriesStart = startDate.Add(timeOfDay);
			}
		}
		catch (COMException)
		{
		}

		var hasPatternSeriesStart = patternSeriesStart.HasValue && patternSeriesStart.Value.Year >= 1900;
		if (!hasPatternSeriesStart)
		{
			patternSeriesStart = null;
		}

		var seriesStart = patternSeriesStart ?? masterStart ?? baseStartLocal;
		var (baseDuration, adjustedSeriesStart) = CalculateBaseDuration(
		pattern,
		masterStart,
		masterEnd,
		apptIsMaster,
		baseStartLocal,
		baseStartUtc,
		baseEndUtc,
		hasPatternSeriesStart,
		seriesStart);
		seriesStart = adjustedSeriesStart;

		if (seriesStart == DateTime.MinValue || seriesStart.Year < 1900)
		{
			seriesStart = baseStartLocal;
		}

		var seriesEnd = seriesStart.Add(baseDuration);
		var seriesStartUtc = ConvertFromSourceLocalToUtc(seriesStart, $"series '{appt.Subject}' anchor start");
		var seriesEndUtc = ConvertFromSourceLocalToUtc(seriesEnd, $"series '{appt.Subject}' anchor end");

		var calEvent = new CalendarEvent
		{
			Start = new CalDateTime(seriesStartUtc),
			End = new CalDateTime(seriesEndUtc),
			RecurrenceRules = new List<RecurrencePattern> { rule }
		};

		return (calEvent, baseDuration);
	}

	private (TimeSpan baseDuration, DateTime seriesStart) CalculateBaseDuration(
	Outlook.RecurrencePattern pattern,
	DateTime? masterStart,
	DateTime? masterEnd,
	bool apptIsMaster,
	DateTime baseStartLocal,
	DateTime baseStartUtc,
	DateTime baseEndUtc,
	bool hasPatternSeriesStart,
	DateTime seriesStart)
	{
		var baseDuration = TimeSpan.Zero;

		if (pattern.StartTime != DateTime.MinValue && pattern.EndTime != DateTime.MinValue)
		{
			var candidate = pattern.EndTime - pattern.StartTime;
			if (candidate > TimeSpan.Zero)
			{
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero && masterStart.HasValue && masterEnd.HasValue)
		{
			var candidate = masterEnd.Value - masterStart.Value;
			if (candidate > TimeSpan.Zero)
			{
				if (!hasPatternSeriesStart)
				{
					seriesStart = masterStart.Value;
				}
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero && apptIsMaster)
		{
			var candidate = baseEndUtc - baseStartUtc;
			if (candidate > TimeSpan.Zero)
			{
				if (!hasPatternSeriesStart)
				{
					seriesStart = baseStartLocal;
				}
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero)
		{
			var candidate = baseEndUtc - baseStartUtc;
			if (candidate > TimeSpan.Zero)
			{
				baseDuration = candidate;
			}
		}

		return (baseDuration, seriesStart);
	}

	private void ProcessRecurrenceExceptions(
	Outlook.RecurrencePattern pattern,
	Outlook.AppointmentItem appt,
	DateTime from,
	DateTime to,
	List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)> results,
	HashSet<DateTime> skipDates)
	{
		foreach (Outlook.Exception ex in pattern.Exceptions)
		{
			try
			{
				skipDates.Add(ex.OriginalDate.Date);

				if (ex.AppointmentItem != null)
				{
					var (exStartLocal, exStartUtc) = NormalizeOutlookTimes(ex.AppointmentItem.Start, ex.AppointmentItem.StartUTC, $"exception '{appt.Subject}' start");
					var (exEndLocal, exEndUtc) = NormalizeOutlookTimes(ex.AppointmentItem.End, ex.AppointmentItem.EndUTC, $"exception '{appt.Subject}' end");

					if (exStartLocal >= from && exStartLocal <= to)
					{
						var exUid = $"outlook-{appt.GlobalAppointmentID}-{exStartLocal:yyyyMMddTHHmmss}";
						results.Add((exUid, exStartLocal, exEndLocal, exStartUtc, exEndUtc));
						_logger.LogInformation("Processed modified occurrence for '{Subject}' at {Start}", appt.Subject, exStartLocal);
					}
				}
			}
			catch
			{
			}
		}
	}

	private void AddCalculatedOccurrences(
	List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)> results,
	Outlook.AppointmentItem appt,
	IEnumerable<Occurrence> occurrences,
	HashSet<DateTime> skipDates,
	TimeSpan baseDuration)
	{
		foreach (var occ in occurrences)
		{
			var startUtc = DateTime.SpecifyKind(occ.Period.StartTime.AsUtc, DateTimeKind.Utc);
			var endUtc = DateTime.SpecifyKind(occ.Period.EndTime?.AsUtc ?? startUtc.Add(baseDuration), DateTimeKind.Utc);
			var startLocal = ConvertUtcToSourceLocal(startUtc);
			var endLocal = ConvertUtcToSourceLocal(endUtc);
			if (skipDates.Contains(startLocal.Date))
			{
				continue;
			}

			var uid = $"outlook-{appt.GlobalAppointmentID}-{startLocal:yyyyMMddTHHmmss}";
			results.Add((uid, startLocal, endLocal, startUtc, endUtc));
		}
	}
}
