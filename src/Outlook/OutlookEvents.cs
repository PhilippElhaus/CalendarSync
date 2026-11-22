using Microsoft.Extensions.Logging;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private OutlookSnapshot GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts,
		DateTime syncStart, DateTime syncEnd, CancellationToken token)
	{
		var events = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var expandedRecurringIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var failedItemCount = 0;

		foreach (var appt in appts)
		{
			token.ThrowIfCancellationRequested();

			try
			{
				if (appt.MeetingStatus is Outlook.OlMeetingStatus.olMeetingCanceled or Outlook.OlMeetingStatus.olMeetingReceivedAndCanceled)
				{
					continue;
				}

				if (appt.IsRecurring)
				{
					if (!ProcessRecurringAppointment(appt, events, expandedRecurringIds, syncStart, syncEnd, token))
					{
						failedItemCount++;
					}
					continue;
				}

				var allDay = appt.AllDayEvent;
				var (startLocal, startUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, allDay);
				var (endLocal, endUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, allDay);

				if (!CalendarRules.OverlapsWindow(startLocal, endLocal, syncStart, syncEnd))
				{
					continue;
				}

				var body = ReadOutlookBodyIfEnabled(appt);
				var dtoSingle = new OutlookEventDto(
					appt.Subject ?? string.Empty,
					body.Body,
					appt.Location ?? string.Empty,
					startLocal,
					endLocal,
					startUtc,
					endUtc,
					appt.GlobalAppointmentID,
					allDay,
					body.WasRead
				);

				AddSnapshotEvent(events, dtoSingle);
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception ex)
			{
				failedItemCount++;
				_logger.LogWarning(ex, "Failed to process one Outlook event. Snapshot is incomplete.");
			}
		}

		return new OutlookSnapshot(events, failedItemCount == 0, failedItemCount, false);
	}

	private void AddSnapshotEvent(Dictionary<string, OutlookEventDto> events, OutlookEventDto dto)
	{
		var sanitizedDto = dto with
		{
			StartLocal = DateTime.SpecifyKind(dto.StartLocal, DateTimeKind.Unspecified),
			EndLocal = DateTime.SpecifyKind(dto.EndLocal, DateTimeKind.Unspecified),
			StartUtc = DateTime.SpecifyKind(dto.StartUtc, DateTimeKind.Utc),
			EndUtc = DateTime.SpecifyKind(dto.EndUtc, DateTimeKind.Utc)
		};

		var uid = CalendarRules.BuildManagedUid(_sourceId, dto.GlobalId, dto.StartUtc);
		if (!events.ContainsKey(uid) &&
			events.Count >= MaxOutlookItemsPerSnapshot)
		{
			throw new InvalidDataException("Outlook snapshot exceeded the event limit.");
		}
		CalendarRules.AddSnapshotEvent(events, uid, sanitizedDto);
	}
}
