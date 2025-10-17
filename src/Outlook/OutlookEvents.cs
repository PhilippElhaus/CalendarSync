using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private Dictionary<string, OutlookEventDto> GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts)
	{
		var events = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var expandedRecurringIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		var sourceToday = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, _sourceTimeZone).Date;
		var syncStart = sourceToday.AddDays(-_config.SyncDaysIntoPast);
		var syncEnd = sourceToday.AddDays(_config.SyncDaysIntoFuture);

		foreach (var appt in appts)
		{
			try
			{
				if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
				{
					continue;
				}

				if (appt.IsRecurring)
				{
					ProcessRecurringAppointment(appt, events, expandedRecurringIds, syncStart, syncEnd);
					continue;
				}

				var (startLocal, startUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"'{appt.Subject}' start");
				var (endLocal, endUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"'{appt.Subject}' end");

				if (endLocal < syncStart || startLocal > syncEnd)
				{
					continue;
				}

				var dtoSingle = new OutlookEventDto(
					appt.Subject ?? string.Empty,
					appt.Body ?? string.Empty,
					appt.Location ?? string.Empty,
					startLocal,
					endLocal,
					startUtc,
					endUtc,
					appt.GlobalAppointmentID ?? Guid.NewGuid().ToString(),
					appt.AllDayEvent
				);

				dtoSingle = EnsureEventConsistency(dtoSingle, $"single '{appt.Subject}'");
				AddEventChunks(events, dtoSingle.GlobalId ?? appt.GlobalAppointmentID ?? Guid.NewGuid().ToString(), dtoSingle);
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to process Outlook event '{Subject}'.", appt.Subject);
			}
		}

		return DeduplicateEvents(events);
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
		var prefix = string.IsNullOrWhiteSpace(_sourceId) ? "outlook" : $"{_sourceId}-outlook";
		var baseKey = string.IsNullOrWhiteSpace(baseUid) ? Guid.Empty.ToString("N") : baseUid;
		var baseHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(baseKey))).ToLowerInvariant();
		var startUtc = dto.StartUtc != DateTime.MinValue ? dto.StartUtc : ConvertFromSourceLocalToUtc(dto.StartLocal, "uid build fallback");
		var occurrenceMarker = startUtc.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
		return $"{prefix}-{baseHash}-{occurrenceMarker}";
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
