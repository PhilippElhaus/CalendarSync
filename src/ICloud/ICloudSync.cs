using Ical.Net;
using Ical.Net.Serialization;
using Microsoft.Extensions.Logging;
using System.Text;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, OutlookEventDto> outlookEvents, CancellationToken token)
	{
		var calendarUrl = BuildCalendarUrl();
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, true, token).ConfigureAwait(false);

		_logger.LogInformation("Found {Count} managed iCloud events before sync.", iCloudEvents.Count);
		_tray.SetUpdating();

		var total = outlookEvents.Count;
		var done = 0;

		foreach (var (uid, dto) in outlookEvents)
		{
			token.ThrowIfCancellationRequested();
			done++;
			if (total > 0)
			{
				_tray.UpdateText($"Updating... {done}/{total} ({done * 100 / total}%)");
			}

			var eventUrl = $"{calendarUrl}{uid}.ics";
			var dtoForWrite = dto;
			if (!dto.BodyWasRead && iCloudEvents.ContainsKey(uid))
			{
				var existingDescription = await GetICloudEventDescriptionAsync(client, eventUrl, token).ConfigureAwait(false);
				dtoForWrite = dto with { Body = existingDescription };
			}

			var calEvent = CreateCalendarEvent(dtoForWrite, uid);
			var calendar = new Calendar { Events = { calEvent } };
			var serializer = new CalendarSerializer();
			var newIcs = serializer.SerializeToString(calendar) ?? string.Empty;

			using (await SendCalDavAsync(
				client,
				() => new HttpRequestMessage(HttpMethod.Put, eventUrl)
				{
					Content = new StringContent(newIcs, Encoding.UTF8, "text/calendar")
				},
				"event upsert",
				token).ConfigureAwait(false))
			{
			}

			_logger.LogDebug("Synced iCloud event UID {Uid}.", uid);
			var verified = await VerifyICloudEventAsync(client, eventUrl, dto, token).ConfigureAwait(false);
			if (!verified)
			{
				var corrected = await AttemptICloudCorrectionAsync(client, eventUrl, newIcs, dto, token).ConfigureAwait(false);
				if (!corrected)
				{
					throw new InvalidDataException($"iCloud event verification failed after correction for UID {uid}.");
				}
			}
		}

		if (total > 0)
		{
			_tray.UpdateText($"Updating... {total}/{total} (100%)");
		}

		// Delete only after a complete Outlook snapshot and every desired event was written and verified.
		var staleUids = CalendarRules.GetStaleManagedUids(_sourceId, iCloudEvents.Keys, outlookEvents.Keys);

		if (staleUids.Count == 0)
		{
			_logger.LogInformation("No stale iCloud events detected after successful upserts.");
			return;
		}

		_logger.LogInformation("Deleting {Count} stale iCloud events after successful upserts.", staleUids.Count);
		_tray.SetDeleting();
		var deleted = 0;

		foreach (var uid in staleUids)
		{
			token.ThrowIfCancellationRequested();
			deleted++;
			_tray.UpdateText($"Deleting... {deleted}/{staleUids.Count} ({deleted * 100 / staleUids.Count}%)");
			var deleteUrl = $"{calendarUrl}{uid}.ics";

			using (await SendCalDavAsync(
				client,
				() => new HttpRequestMessage(HttpMethod.Delete, deleteUrl),
				"stale event deletion",
				token).ConfigureAwait(false))
			{
			}

			_logger.LogDebug("Deleted stale iCloud event UID {Uid}.", uid);
		}
	}
}
