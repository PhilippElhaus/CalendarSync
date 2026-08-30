namespace CalendarSync;

internal sealed record OutlookEventDto(
	string Subject,
	string Body,
	string Location,
	DateTime StartLocal,
	DateTime EndLocal,
	DateTime StartUtc,
	DateTime EndUtc,
	string GlobalId,
	bool IsAllDay,
	bool BodyWasRead);

internal sealed record OutlookSnapshot(
	Dictionary<string, OutlookEventDto> Events,
	bool IsComplete,
	int FailedItemCount,
	bool HitItemLimit)
{
	public static OutlookSnapshot Incomplete(int failedItemCount = 1) =>
		new(new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase), false, failedItemCount, false);
}
