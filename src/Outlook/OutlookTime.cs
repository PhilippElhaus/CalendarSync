using Microsoft.Extensions.Logging;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private (DateTime local, DateTime utc) NormalizeOutlookTimes(DateTime local, DateTime utc, bool allDay) =>
		CalendarRules.NormalizeOutlookTime(local, utc, _sourceTimeZone, allDay);

	private static bool DetermineAllDay(DateTime startLocal, DateTime endLocal, bool flaggedAllDay) =>
		CalendarRules.DetermineAllDay(startLocal, endLocal, flaggedAllDay);

	private static (DateTime startDate, DateTime endDate) GetAllDayDateRange(DateTime startLocal, DateTime endLocal) =>
		CalendarRules.GetAllDayDateRange(startLocal, endLocal);

	private DateTime ConvertFromSourceLocalToUtc(DateTime local) => CalendarRules.ConvertSourceLocalToUtc(local, _sourceTimeZone);

	private DateTime ConvertUtcToSourceLocal(DateTime utc) => CalendarRules.ConvertUtcToSourceLocal(utc, _sourceTimeZone);

	private TimeZoneInfo ResolveTimeZone(string? timeZoneId, string role)
	{
		_logger.LogInformation("Time-zone configuration for {Role}: Configured={Configured}.", role, !string.IsNullOrWhiteSpace(timeZoneId));
		return string.IsNullOrWhiteSpace(timeZoneId) ? TimeZoneInfo.Local : TimeZoneInfo.FindSystemTimeZoneById(timeZoneId.Trim());
	}
}
