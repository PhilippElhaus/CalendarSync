using Ical.Net;
using Ical.Net.DataTypes;
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

	private RecurrencePattern? BuildRecurrenceRule(Outlook.RecurrencePattern pattern, string? subject)
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
				_logger.LogWarning("Unsupported recurrence type for event '{Subject}'. Skipping.", subject);
				return null;
		}

		ApplyPatternEnd(rule, pattern);
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

	private void ProcessRecurrenceExceptions(
                Outlook.RecurrencePattern pattern,
                Outlook.AppointmentItem appt,
                DateTime from,
                DateTime to,
                List<OccurrenceInfo> results,
                HashSet<DateTime> skipDates)
	{
                foreach (Outlook.Exception ex in pattern.Exceptions)
                {
                        try
                        {
                                skipDates.Add(ex.OriginalDate.Date);

                                var exceptionItem = ex.AppointmentItem;
                                if (exceptionItem != null)
                                {
                                        try
                                        {
                                                var (exStartLocal, exStartUtc) = NormalizeOutlookTimes(exceptionItem.Start, exceptionItem.StartUTC, $"exception '{appt.Subject}' start");
                                                var (exEndLocal, exEndUtc) = NormalizeOutlookTimes(exceptionItem.End, exceptionItem.EndUTC, $"exception '{appt.Subject}' end");

                                                if (exStartLocal >= from && exStartLocal <= to)
                                                {
                                                        var exAllDay = DetermineAllDay(exStartLocal, exEndLocal, exceptionItem.AllDayEvent);
                                                        results.Add(new OccurrenceInfo(
                                                                exStartLocal,
                                                                exEndLocal,
                                                                exStartUtc,
                                                                exEndUtc,
                                                                exAllDay,
                                                                exceptionItem.Subject,
                                                                exceptionItem.Body,
                                                                exceptionItem.Location));
                                                        _logger.LogInformation("Processed modified occurrence for '{Subject}' at {Start}", appt.Subject, exStartLocal);
                                                }
                                        }
                                        finally
                                        {
                                                try
                                                {
                                                        Marshal.FinalReleaseComObject(exceptionItem);
                                                }
                                                catch
                                                {
                                                }
                                        }
                                }
                        }
                        catch
                        {
                        }
                }
        }

	private void AddCalculatedOccurrences(
                List<OccurrenceInfo> results,
                Outlook.AppointmentItem appt,
                IEnumerable<Occurrence> occurrences,
                HashSet<DateTime> skipDates,
                TimeSpan baseDuration,
                bool seriesAllDay)
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

                        var occAllDay = DetermineAllDay(startLocal, endLocal, seriesAllDay);
                        results.Add(new OccurrenceInfo(startLocal, endLocal, startUtc, endUtc, occAllDay, null, null, null));
                }
        }
