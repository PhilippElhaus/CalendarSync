using Microsoft.Extensions.Logging;
namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task WipeICloudCalendarAsync(HttpClient client, string calendarUrl, CancellationToken token, bool filterBySource)
	{
		if (filterBySource)
		{
			_logger.LogInformation("Cleaning existing events for source {SourceId}.", _sourceId);
		}
		else
		{
			_logger.LogInformation("Cleaning all existing iCloud events.");
		}

		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, filterBySource, token).ConfigureAwait(false);
		_logger.LogInformation("Found {Count} existing iCloud events to delete.", iCloudEvents.Count);

		_tray.SetDeleting();
		var total = iCloudEvents.Count;
		var done = 0;

		foreach (var iCloudUid in iCloudEvents.Keys.ToList())
		{
			done++;
			if (total > 0)
			{
				_tray.UpdateText($"Deleting... {done}/{total} ({done * 100 / total}%)");
			}

			var eventUrl = $"{calendarUrl}{iCloudUid}.ics";
			await Task.Delay(300, token).ConfigureAwait(false);

			using (await SendCalDavAsync(
				client,
				() => new HttpRequestMessage(HttpMethod.Delete, eventUrl),
				"calendar wipe deletion",
				token).ConfigureAwait(false))
			{
			}

			_logger.LogDebug("Deleted iCloud event UID {Uid} during wipe.", iCloudUid);
		}

		if (total > 0)
		{
			_tray.UpdateText("Finalizing cleaning run...");
		}

		_logger.LogInformation("Finished iCloud calendar wipe. Waiting 30 seconds for cache to clear.");
		await Task.Delay(TimeSpan.FromSeconds(30), token).ConfigureAwait(false);
	}
}
