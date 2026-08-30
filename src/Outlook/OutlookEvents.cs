using Microsoft.Extensions.Logging;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private OutlookSnapshot GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts, CancellationToken token)
	{
		var events = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var expandedRecurringIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var failedItemCount = 0;

		var sourceToday = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, _sourceTimeZone).Date;
		var syncStart = sourceToday.AddDays(-_config.SyncDaysIntoPast);
		var syncEnd = sourceToday.AddDays(_config.SyncDaysIntoFuture);

		foreach (var appt in appts)
		{
			token.ThrowIfCancellationRequested();

			try
			{
				if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
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

				var (startLocal, startUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"'{appt.Subject}' start");
				var (endLocal, endUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"'{appt.Subject}' end");

				if (endLocal < syncStart || startLocal > syncEnd)
				{
					continue;
				}

				var body = ReadOutlookBodyIfEnabled(appt, $"single '{appt.Subject}'");
				var dtoSingle = new OutlookEventDto(
					appt.Subject ?? string.Empty,
					body.Body,
					appt.Location ?? string.Empty,
					startLocal,
					endLocal,
					startUtc,
					endUtc,
					appt.GlobalAppointmentID ?? Guid.NewGuid().ToString(),
					appt.AllDayEvent,
					body.WasRead
				);

				dtoSingle = EnsureEventConsistency(dtoSingle, $"single '{appt.Subject}'");
				AddEventChunks(events, dtoSingle.GlobalId ?? appt.GlobalAppointmentID ?? Guid.NewGuid().ToString(), dtoSingle);
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

		var deduplicated = DeduplicateEvents(events);
		return new OutlookSnapshot(deduplicated, failedItemCount == 0, failedItemCount, false);
	}

	private void AddEventChunks(Dictionary<string, OutlookEventDto> events, string baseUid, OutlookEventDto dto)
	{
		var sanitizedDto = dto with
		{
			StartLocal = DateTime.SpecifyKind(dto.StartLocal, DateTimeKind.Unspecified),
			EndLocal = DateTime.SpecifyKind(dto.EndLocal, DateTimeKind.Unspecified),
			StartUtc = DateTime.SpecifyKind(dto.StartUtc, DateTimeKind.Utc),
			EndUtc = DateTime.SpecifyKind(dto.EndUtc, DateTimeKind.Utc)
		};

		var managedUid = BuildManagedUid(baseUid, sanitizedDto);
		events[managedUid] = sanitizedDto;
	}

	private string BuildManagedUid(string baseUid, OutlookEventDto dto)
	{
		var startUtc = dto.StartUtc != DateTime.MinValue ? dto.StartUtc : ConvertFromSourceLocalToUtc(dto.StartLocal, "uid build fallback");
		return CalendarRules.BuildManagedUid(_sourceId, baseUid, startUtc);
	}

	private Dictionary<string, OutlookEventDto> DeduplicateEvents(Dictionary<string, OutlookEventDto> events)
	{
		var deduped = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		foreach (var (uid, dto) in events)
		{
			if (dto == null)
			{
				continue;
			}

			var globalId = dto.GlobalId ?? string.Empty;
			var signature = $"{globalId}|{dto.StartUtc:O}|{dto.EndUtc:O}";

			if (!seenKeys.Add(signature))
			{
				_logger.LogWarning("Detected duplicate Outlook event for GlobalID {GlobalId} at {Start}. Dropping UID {Uid}.", globalId, dto.StartLocal, uid);
				continue;
			}

			deduped[uid] = dto;
		}

		return deduped;
	}
}
