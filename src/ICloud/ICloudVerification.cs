using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;

namespace CalendarSync;

public partial class CalendarSyncService
{
	internal static bool MatchesEvent(CalendarEvent expected, CalendarEvent actual)
	{
		return string.Equals(expected.Uid, actual.Uid, StringComparison.Ordinal) &&
			expected.IsAllDay == actual.IsAllDay &&
			MatchesTime(expected.Start, actual.Start) && MatchesTime(expected.End, actual.End) &&
			NormalizeCalendarText(expected.Summary) == NormalizeCalendarText(actual.Summary) &&
			NormalizeCalendarText(expected.Location) == NormalizeCalendarText(actual.Location) &&
			NormalizeCalendarText(expected.Description) == NormalizeCalendarText(actual.Description) &&
			!new[] { "RRULE", "RDATE", "EXRULE", "EXDATE", "RECURRENCE-ID" }.Any(actual.Properties.ContainsKey) &&
			expected.Alarms.Select(AlarmKey).Order().SequenceEqual(actual.Alarms.Select(AlarmKey).Order());
	}

	private static string NormalizeCalendarText(string? text) => (text ?? string.Empty).ReplaceLineEndings("\n");

	private static (string Action, string? Duration, DateTime? Absolute, string Related, int Repeat) AlarmKey(Alarm alarm) =>
		(alarm.Action ?? string.Empty, alarm.Trigger?.Duration?.ToString(), alarm.Trigger?.DateTime?.Value,
			alarm.Trigger?.Related.ToString() ?? "Start", alarm.Repeat);

	private static bool MatchesTime(CalDateTime? expected, CalDateTime? actual)
	{
		if (expected == null || actual == null || expected.HasTime != actual.HasTime)
		{
			return false;
		}
		return expected.HasTime
			? actual.TzId != null && expected.AsUtc.Ticks / TimeSpan.TicksPerSecond == actual.AsUtc.Ticks / TimeSpan.TicksPerSecond
			: expected.Value.Date == actual.Value.Date;
	}
}
