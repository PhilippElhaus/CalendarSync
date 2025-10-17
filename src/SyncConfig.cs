namespace CalendarSync;

public class SyncConfig
{
	public string? ICloudCalDavUrl { get; set; }
	public string? ICloudUser { get; set; }
	public string? ICloudPassword { get; set; }
	public string? PrincipalId { get; set; }
	public string? WorkCalendarId { get; set; }
	public string LogLevel { get; set; } = "Information";
	public int InitialWaitSeconds { get; set; } = 60;
	public int SyncIntervalMinutes { get; set; } = 3;
	public int SyncDaysIntoFuture { get; set; } = 30;
	public int SyncDaysIntoPast { get; set; } = 30;
	public string? SourceId { get; set; }
	public string? EventTag { get; set; }
	public string? SourceTimeZoneId { get; set; }
	public string? TargetTimeZoneId { get; set; }
	public int RecurrenceExpansionDaysPast { get; set; } = 30;
	public int RecurrenceExpansionDaysFuture { get; set; } = 30;
	public bool IncludeSecondReminder { get; set; } = true;
}
