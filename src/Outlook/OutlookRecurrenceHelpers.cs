using Ical.Net;
using Ical.Net.DataTypes;
using Ical.Net.Evaluation;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

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

	private RecurrencePattern? BuildRecurrenceRule(Outlook.RecurrencePattern pattern, bool seriesAllDay)
	{
		var rule = new RecurrencePattern
		{
			Interval = Math.Max(1, pattern.Interval)
		};

		switch (pattern.RecurrenceType)
		{
			case Outlook.OlRecurrenceType.olRecursDaily:
			rule.Frequency = FrequencyType.Daily;
			break;
			case Outlook.OlRecurrenceType.olRecursWeekly:
			rule.Frequency = FrequencyType.Weekly;
			ConfigureWeeklyRule(rule, pattern.DayOfWeekMask);
			break;
			case Outlook.OlRecurrenceType.olRecursMonthly:
			rule.Frequency = FrequencyType.Monthly;
			ConfigureMonthlyRule(rule, pattern.DayOfMonth);
			break;
			case Outlook.OlRecurrenceType.olRecursMonthNth:
			rule.Frequency = FrequencyType.Monthly;
			ConfigureNthRule(rule, pattern.DayOfWeekMask, pattern.Instance);
			break;
			case Outlook.OlRecurrenceType.olRecursYearly:
			rule.Frequency = FrequencyType.Yearly;
			ConfigureYearlyRule(rule, pattern.MonthOfYear, pattern.DayOfMonth);
			break;
			case Outlook.OlRecurrenceType.olRecursYearNth:
			rule.Frequency = FrequencyType.Yearly;
			ConfigureYearlyNthRule(rule, pattern.MonthOfYear, pattern.DayOfWeekMask, pattern.Instance);
			break;
			default:
			_logger.LogWarning("Outlook recurrence type is unsupported. Snapshot is incomplete.");
			return null;
		}

		ApplyPatternEnd(rule, pattern, seriesAllDay);
		return rule;
	}

	private void ConfigureWeeklyRule(RecurrencePattern rule, Outlook.OlDaysOfWeek mask)
	{
		var byDay = GetWeekDaysFromMask(mask, null);
		if (byDay.Count == 0)
		{
			return;
		}
		rule.ByDay = byDay;
	}

	private void ConfigureMonthlyRule(RecurrencePattern rule, int dayOfMonth)
	{
		if (dayOfMonth >= 1 && dayOfMonth <= 31)
		{
			rule.ByMonthDay = new List<int> { dayOfMonth };
		}
	}

	private void ConfigureNthRule(RecurrencePattern rule, Outlook.OlDaysOfWeek mask, int instance)
	{
		var occurrence = NormalizeInstance(instance);
		var byDay = GetWeekDaysFromMask(mask, occurrence);
		if (byDay.Count == 0)
		{
			return;
		}
		rule.ByDay = byDay;
	}

	private void ConfigureYearlyRule(RecurrencePattern rule, int monthOfYear, int dayOfMonth)
	{
		if (monthOfYear >= 1 && monthOfYear <= 12)
		{
			rule.ByMonth = new List<int> { monthOfYear };
		}
		if (dayOfMonth >= 1 && dayOfMonth <= 31)
		{
			rule.ByMonthDay = new List<int> { dayOfMonth };
		}
	}

	private void ConfigureYearlyNthRule(RecurrencePattern rule, int monthOfYear, Outlook.OlDaysOfWeek mask, int instance)
	{
		if (monthOfYear >= 1 && monthOfYear <= 12)
		{
			rule.ByMonth = new List<int> { monthOfYear };
		}
		var occurrence = NormalizeInstance(instance);
		var byDay = GetWeekDaysFromMask(mask, occurrence);
		if (byDay.Count == 0)
		{
			return;
		}
		rule.ByDay = byDay;
	}

	private List<WeekDay> GetWeekDaysFromMask(Outlook.OlDaysOfWeek mask, int? occurrence)
	{
		var byDay = new List<WeekDay>();
		void Add(DayOfWeek day)
		{
			var weekDay = occurrence.HasValue && occurrence.Value != 0
				? new WeekDay(day, occurrence.Value)
				: new WeekDay(day);
			byDay.Add(weekDay);
		}

		if ((mask & Outlook.OlDaysOfWeek.olMonday) != 0)
		{
			Add(DayOfWeek.Monday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olTuesday) != 0)
		{
			Add(DayOfWeek.Tuesday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olWednesday) != 0)
		{
			Add(DayOfWeek.Wednesday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olThursday) != 0)
		{
			Add(DayOfWeek.Thursday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olFriday) != 0)
		{
			Add(DayOfWeek.Friday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olSaturday) != 0)
		{
			Add(DayOfWeek.Saturday);
		}
		if ((mask & Outlook.OlDaysOfWeek.olSunday) != 0)
		{
			Add(DayOfWeek.Sunday);
		}

		return byDay;
	}

	private int NormalizeInstance(int instance)
	{
		if (instance <= 0)
		{
			return 0;
		}
		return instance == 5 ? -1 : instance;
	}

	private void ApplyPatternEnd(RecurrencePattern rule, Outlook.RecurrencePattern pattern, bool seriesAllDay)
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
			var patternEndDate = DateTime.SpecifyKind(pattern.PatternEndDate.Date, DateTimeKind.Unspecified);
			rule.Until = seriesAllDay
				? new CalDateTime(patternEndDate, false)
				: new CalDateTime(patternEndDate.AddDays(1).AddTicks(-1), true);
		}
	}

	private bool ProcessRecurrenceExceptions(
				Outlook.RecurrencePattern pattern,
				DateTime from,
				DateTime to,
				List<OccurrenceInfo> results,
				HashSet<DateTime> skipDates,
				CancellationToken token)
	{
		Outlook.Exceptions? exceptions = null;
		var isComplete = true;
		try
		{
			exceptions = pattern.Exceptions;
			var exceptionCount = exceptions.Count;

			for (var exceptionIndex = 1; exceptionIndex <= exceptionCount; exceptionIndex++)
			{
				token.ThrowIfCancellationRequested();

				Outlook.Exception? recurrenceException = null;
				Outlook.AppointmentItem? exceptionItem = null;
				try
				{
					recurrenceException = exceptions[exceptionIndex];
					skipDates.Add(recurrenceException.OriginalDate.Date);

					if (recurrenceException.Deleted)
					{
						continue;
					}

					exceptionItem = recurrenceException.AppointmentItem;
					if (exceptionItem == null)
					{
						isComplete = false;
						continue;
					}

					token.ThrowIfCancellationRequested();

					if (exceptionItem.MeetingStatus is Outlook.OlMeetingStatus.olMeetingCanceled or Outlook.OlMeetingStatus.olMeetingReceivedAndCanceled)
					{
						continue;
					}
					var allDay = exceptionItem.AllDayEvent;
					var (exStartLocal, exStartUtc) = NormalizeOutlookTimes(exceptionItem.Start, exceptionItem.StartUTC, allDay);
					var (exEndLocal, exEndUtc) = NormalizeOutlookTimes(exceptionItem.End, exceptionItem.EndUTC, allDay);

					if (CalendarRules.OverlapsWindow(exStartLocal, exEndLocal, from, to))
					{
						if (results.Count >= MaxOutlookItemsPerSnapshot)
						{
							return false;
						}
						var exAllDay = DetermineAllDay(exStartLocal, exEndLocal, allDay);
						var body = ReadOutlookBodyIfEnabled(exceptionItem);
						results.Add(new OccurrenceInfo(
								exStartLocal,
								exEndLocal,
								exStartUtc,
								exEndUtc,
								exAllDay,
								exceptionItem.Subject,
								body.Body,
								exceptionItem.Location,
								body.WasRead));
					}
				}
				catch (OperationCanceledException)
				{
					throw;
				}
				catch (COMException ex)
				{
					isComplete = false;
					_logger.LogDebug("Outlook recurrence exception unavailable. Code=0x{Code:X8}.", ex.HResult);
				}
				finally
				{
					ReleaseComObject(exceptionItem, "recurrence exception appointment");
					ReleaseComObject(recurrenceException, "recurrence exception");
				}
			}
		}
		finally
		{
			ReleaseComObject(exceptions, "recurrence exceptions collection");
		}

		return isComplete;
	}

	private bool AddCalculatedOccurrences(
			List<OccurrenceInfo> results,
			IEnumerable<Occurrence> occurrences,
			HashSet<DateTime> skipDates,
			TimeSpan baseDuration,
			TimeSpan baseLocalDuration,
			DateTime baseStartLocal,
			bool seriesAllDay,
			CancellationToken token)
	{
		try
		{
			foreach (var occ in occurrences)
			{
				token.ThrowIfCancellationRequested();

				var periodStart = occ.Period.StartTime;
				if (periodStart == null)
				{
					continue;
				}

				var startLocal = DateTime.SpecifyKind(periodStart.Value, DateTimeKind.Unspecified);
				var endLocal = DateTime.SpecifyKind(occ.Period.EndTime?.Value ?? startLocal.Add(baseLocalDuration), DateTimeKind.Unspecified);
				if (endLocal <= startLocal)
				{
					var fallbackDuration = baseLocalDuration > TimeSpan.Zero ? baseLocalDuration : baseDuration;
					endLocal = startLocal.Add(fallbackDuration);
				}

				if (skipDates.Contains(startLocal.Date))
				{
					continue;
				}

				if (!seriesAllDay)
				{
					var desiredStartLocal = DateTime.SpecifyKind(startLocal.Date.Add(baseStartLocal.TimeOfDay), DateTimeKind.Unspecified);
					var desiredEndLocal = desiredStartLocal + baseLocalDuration;

					startLocal = desiredStartLocal;
					endLocal = desiredEndLocal;
				}

				if (_sourceTimeZone.IsInvalidTime(startLocal) || _sourceTimeZone.IsInvalidTime(endLocal))
				{
					_logger.LogWarning("Outlook recurrence falls in a time-zone gap. Snapshot is incomplete.");
					return false;
				}

				if (results.Count >= MaxOutlookItemsPerSnapshot)
				{
					return false;
				}
				var startUtc = ConvertFromSourceLocalToUtc(startLocal);
				var endUtc = ConvertFromSourceLocalToUtc(endLocal);
				var occAllDay = DetermineAllDay(startLocal, endLocal, seriesAllDay);
				results.Add(new OccurrenceInfo(startLocal, endLocal, startUtc, endUtc, occAllDay, null, null, null, false));
			}

			return true;
		}
		catch (EvaluationOutOfRangeException)
		{
			_logger.LogWarning("Outlook recurrence reached the evaluation limit. Snapshot is incomplete.");
			return false;
		}
	}
}
