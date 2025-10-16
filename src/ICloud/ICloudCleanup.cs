using System.Net;
using System.Net.Http;
using System.Threading;

namespace CalendarSync.src;

public partial class CalendarSyncService
{
	private async Task WipeICloudCalendarAsync(HttpClient client, string calendarUrl, CancellationToken token, bool filterBySource)
	{
		if (filterBySource)
		_logger.LogInformation("Cleaning existing events for source {SourceId}.", _sourceId);
		else
		_logger.LogInformation("Cleaning all existing iCloud events.");

		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, filterBySource);
		_logger.LogInformation("Found {Count} existing iCloud events to delete.", iCloudEvents.Count);

		_tray.SetDeleting();
		var total = iCloudEvents.Count;
		var done = 0;

		foreach (var iCloudUid in iCloudEvents.Keys)
		{
			done++;
			if (total > 0)
			_tray.UpdateText($"Deleting... {done}/{total} ({done * 100 / total}%)");

			var eventUrl = $"{calendarUrl}{iCloudUid}.ics";
			var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
			await Task.Delay(300, token);
			try
			{
				var deleteResponse = await client.SendAsync(deleteRequest);
				if (deleteResponse.IsSuccessStatusCode)
				_logger.LogInformation("Deleted iCloud event with UID {Uid}", iCloudUid);
				else
				{
					if (deleteResponse.StatusCode == HttpStatusCode.Unauthorized || deleteResponse.StatusCode == HttpStatusCode.Forbidden)
					throw new UnauthorizedAccessException("iCloud authentication failed.");
					_logger.LogWarning("Failed to delete iCloud event UID {Uid}: {Status} - {Reason}", iCloudUid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
				}
		}
	catch (Exception ex)
	{
		_logger.LogError(ex, "Exception while deleting iCloud event UID {Uid}", iCloudUid);
		await Task.Delay(5000, token);
	}
}

if (total > 0)
_tray.UpdateText("Finalzing cleaning run...");

_logger.LogInformation("Finished full iCloud calendar wipe. Waiting 2 minutes for cache to clear.");
await Task.Delay(TimeSpan.FromSeconds(30), token);
}
}
