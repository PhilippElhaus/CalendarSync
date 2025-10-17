using System.Collections.Generic;
using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private List<(DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc, bool isAllDay)> ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to)
	{
		var results = new List<(DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc, bool isAllDay)>();

		var pattern = TryGetRecurrencePattern(appt);
		if (pattern == null)
		{
			return results;
		}

		var rule = BuildRecurrenceRule(pattern, appt.Subject);
		if (rule == null)
		{
			return results;
		}

		var (baseStartLocal, baseStartUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"series '{appt.Subject}' start");
		var (baseEndLocal, baseEndUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"series '{appt.Subject}' end");
		var seriesAllDay = DetermineAllDay(baseStartLocal, baseEndLocal, appt.AllDayEvent);
		var baseDuration = baseEndUtc - baseStartUtc;
		if (baseDuration <= TimeSpan.Zero)
		{
			_logger.LogWarning("Recurrence duration invalid for '{Subject}'. Falling back to 30 minutes.", appt.Subject);
			baseDuration = TimeSpan.FromMinutes(30);
		}

		CalDateTime startCal;
		CalDateTime endCal;
		if (seriesAllDay)
		{
			var (startDate, endDate) = GetAllDayDateRange(baseStartLocal, baseEndLocal);
			startCal = new CalDateTime(startDate.Year, startDate.Month, startDate.Day) { IsAllDay = true };
			endCal = new CalDateTime(endDate.Year, endDate.Month, endDate.Day) { IsAllDay = true };
		}
		else
		{
			startCal = new CalDateTime(baseStartUtc) { IsUniversalTime = true };
			endCal = new CalDateTime(baseEndUtc) { IsUniversalTime = true };
		}

		var calEvent = new CalendarEvent
		{
			Start = startCal,
			End = endCal,
			RecurrenceRules = new List<RecurrencePattern> { rule },
			IsAllDay = seriesAllDay
		};

		var skipDates = new HashSet<DateTime>();
		ProcessRecurrenceExceptions(pattern, appt, from, to, results, skipDates);

		var utcFrom = ConvertFromSourceLocalToUtc(from);
		var utcTo = ConvertFromSourceLocalToUtc(to);
		var occurrences = calEvent.GetOccurrences(utcFrom, utcTo);
		AddCalculatedOccurrences(results, appt, occurrences, skipDates, baseDuration, seriesAllDay);

		return results;
	}
}
