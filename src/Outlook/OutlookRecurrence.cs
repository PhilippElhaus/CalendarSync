using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
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
		string? LocationOverride,
		bool BodyWasRead);
	private sealed record RecurrenceExpansionResult(List<OccurrenceInfo> Occurrences, bool IsComplete);

	private RecurrenceExpansionResult ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to, CancellationToken token)
	{
		var results = new List<OccurrenceInfo>();
		token.ThrowIfCancellationRequested();

		var pattern = TryGetRecurrencePattern(appt);
		if (pattern == null)
		{
			return new RecurrenceExpansionResult(results, false);
		}

		try
		{
			var allDay = appt.AllDayEvent;
			var (baseStartLocal, baseStartUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, allDay);
			var (baseEndLocal, baseEndUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, allDay);
			var seriesAllDay = DetermineAllDay(baseStartLocal, baseEndLocal, allDay);

			var rule = BuildRecurrenceRule(pattern, seriesAllDay);
			if (rule == null)
			{
				return new RecurrenceExpansionResult(results, false);
			}

			var baseDuration = baseEndUtc - baseStartUtc;
			if (baseDuration < TimeSpan.Zero)
			{
				return new RecurrenceExpansionResult(results, false);
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
			var exceptionsComplete = ProcessRecurrenceExceptions(pattern, from, to, results, skipDates, token);

			token.ThrowIfCancellationRequested();

			var fromLocal = DateTime.SpecifyKind(from - baseLocalDuration, DateTimeKind.Unspecified);
			var toLocal = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);
			var fromCal = new CalDateTime(fromLocal, !seriesAllDay);
			var toCal = new CalDateTime(toLocal, !seriesAllDay);
			var occurrences = calEvent
				.GetOccurrences(fromCal)
				.TakeWhile(o => o.Period.StartTime == null || o.Period.StartTime.CompareTo(toCal) < 0)
				.Where(o => o.Period.StartTime != null);
			var calculatedComplete = AddCalculatedOccurrences(results, occurrences, skipDates, baseDuration, baseLocalDuration, baseStartLocal, seriesAllDay, token);

			return new RecurrenceExpansionResult(results, exceptionsComplete && calculatedComplete);
		}
		finally
		{
			ReleaseComObject(pattern, "Outlook recurrence pattern");
		}
	}
}
