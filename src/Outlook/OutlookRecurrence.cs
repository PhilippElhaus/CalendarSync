using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Microsoft.Extensions.Logging;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private sealed record OccurrenceInfo(
		DateTime StartLocal,
		DateTime EndLocal,
		DateTime StartUtc,
		DateTime EndUtc,
		bool IsAllDay,
		string? SubjectOverride,
		string? BodyOverride,
		string? LocationOverride);

	private List<OccurrenceInfo> ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to)
	{
		var results = new List<OccurrenceInfo>();

		var pattern = TryGetRecurrencePattern(appt);
		if (pattern == null)
		{
			return results;
		}

		var (baseStartLocal, baseStartUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"series '{appt.Subject}' start");
		var (baseEndLocal, baseEndUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"series '{appt.Subject}' end");
		var seriesAllDay = DetermineAllDay(baseStartLocal, baseEndLocal, appt.AllDayEvent);

		var rule = BuildRecurrenceRule(pattern, appt.Subject, seriesAllDay);
		if (rule == null)
		{
			return results;
		}

		var baseDuration = baseEndUtc - baseStartUtc;
		if (baseDuration <= TimeSpan.Zero)
		{
			_logger.LogWarning("Recurrence duration invalid for '{Subject}'. Falling back to 30 minutes.", appt.Subject);
			baseDuration = TimeSpan.FromMinutes(30);
		}

		var baseLocalDuration = baseEndLocal - baseStartLocal;
		if (baseLocalDuration <= TimeSpan.Zero)
		{
			baseLocalDuration = baseDuration;
		}

		CalDateTime startCal;
		CalDateTime endCal;
		if (seriesAllDay)
		{
			var (startDate, endDate) = GetAllDayDateRange(baseStartLocal, baseEndLocal);
			startCal = new CalDateTime(startDate, false);
			endCal = new CalDateTime(endDate, false);
		}
		else
		{
			startCal = new CalDateTime(DateTime.SpecifyKind(baseStartLocal, DateTimeKind.Unspecified), true);
			endCal = new CalDateTime(DateTime.SpecifyKind(baseEndLocal, DateTimeKind.Unspecified), true);
		}

		var calEvent = new CalendarEvent
		{
			Start = startCal,
			End = endCal,
			RecurrenceRule = rule
		};

		var skipDates = new HashSet<DateTime>();
		ProcessRecurrenceExceptions(pattern, appt, from, to, results, skipDates);

		var fromLocal = DateTime.SpecifyKind(from, DateTimeKind.Unspecified);
		var toLocal = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);
		var fromCal = new CalDateTime(fromLocal, !seriesAllDay);
		var toCal = new CalDateTime(toLocal, !seriesAllDay);
		var occurrences = calEvent
			.GetOccurrences(fromCal)
			.TakeWhile(o => o.Period.StartTime == null || o.Period.StartTime.CompareTo(toCal) < 0)
			.Where(o => o.Period.StartTime != null);
		AddCalculatedOccurrences(results, appt, occurrences, skipDates, baseDuration, baseLocalDuration, baseStartLocal, seriesAllDay);

		return results;
	}
}
