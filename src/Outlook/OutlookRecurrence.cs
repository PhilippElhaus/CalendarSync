using System.Runtime.InteropServices;
using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)> ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to)
	{
		var results = new List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)>();

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

		var (_, masterStart, masterEnd, apptIsMaster) = ResolveSeriesContext(appt, pattern, baseStartLocal, baseEndLocal);
		var (calEvent, baseDuration) = CreateRecurrenceCalendarEvent(
			rule,
			baseStartLocal,
			baseStartUtc,
			baseEndUtc,
			masterStart,
			masterEnd,
			pattern,
			appt,
			apptIsMaster);

		var skipDates = new HashSet<DateTime>();
		ProcessRecurrenceExceptions(pattern, appt, from, to, results, skipDates);

		var occurrences = calEvent.GetOccurrences(ConvertFromSourceLocalToUtc(from), ConvertFromSourceLocalToUtc(to));
		AddCalculatedOccurrences(results, appt, occurrences, skipDates, baseDuration);

		return results;
	}
}
