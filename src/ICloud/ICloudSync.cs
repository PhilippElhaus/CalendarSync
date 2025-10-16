using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using Ical.Net;
using Ical.Net.Serialization;

namespace CalendarSync.src;

public partial class CalendarSyncService
{
	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, OutlookEventDto> outlookEvents, CancellationToken token)
	{
		var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, true);

		_logger.LogInformation("Found {Count} iCloud events before sync.", iCloudEvents.Count);

		var desiredUids = new HashSet<string>(outlookEvents.Keys, StringComparer.OrdinalIgnoreCase);
		var staleUids = iCloudEvents.Keys.Where(uid => IsManagedUid(uid) && !desiredUids.Contains(uid)).ToList();

		if (staleUids.Count > 0)
		{
			_logger.LogInformation("Deleting {Count} stale iCloud events before applying updates.", staleUids.Count);
			_tray.SetDeleting();
			var delTotal = staleUids.Count;
			var delDone = 0;

			foreach (var uid in staleUids)
			{
				token.ThrowIfCancellationRequested();

				delDone++;
				_tray.UpdateText($"Deleting... {delDone}/{delTotal} ({delDone * 100 / delTotal}%)");

				var deleteUrl = $"{calendarUrl}{uid}.ics";
				var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, deleteUrl);
				var deleteResponse = await client.SendAsync(deleteRequest, token);

				if (deleteResponse.IsSuccessStatusCode)
				{
					_logger.LogInformation("Deleted stale iCloud event UID {Uid}", uid);
				}
			else
			{
				if (deleteResponse.StatusCode == HttpStatusCode.Unauthorized || deleteResponse.StatusCode == HttpStatusCode.Forbidden)
				throw new UnauthorizedAccessException("iCloud authentication failed.");
				_logger.LogWarning("Failed to delete stale iCloud event UID {Uid}: {Status} - {Reason}", uid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
				await RetryRequestAsync(client, deleteRequest, token);
			}
	}

_tray.SetUpdating();
}
else
{
	_logger.LogInformation("No stale iCloud events detected prior to sync.");
	_tray.SetUpdating();
}

var total = outlookEvents.Count;
var done = 0;

foreach (var (uid, dto) in outlookEvents)
{
	token.ThrowIfCancellationRequested();
	if (dto == null)
	continue;

	done++;
	if (total > 0)
	_tray.UpdateText($"Updating... {done}/{total} ({done * 100 / total}%)");

	var calEvent = CreateCalendarEvent(dto, uid);
	var calendar = new Calendar { Events = { calEvent } };
	var serializer = new CalendarSerializer();
	var newIcs = serializer.SerializeToString(calendar);

	var eventUrl = $"{calendarUrl}{uid}.ics";

	var requestPut = new HttpRequestMessage(HttpMethod.Put, eventUrl)
	{
		Content = new StringContent(newIcs, Encoding.UTF8, "text/calendar")
	};

var responsePut = await client.SendAsync(requestPut, token);
if (responsePut.IsSuccessStatusCode)
{
	_logger.LogInformation("Synced event '{Subject}'", dto.Subject);
	var verified = await VerifyICloudEventAsync(client, eventUrl, dto, token);
	if (!verified)
	{
		await AttemptICloudCorrectionAsync(client, eventUrl, newIcs, dto, token);
	}
}
else
{
	if (responsePut.StatusCode == HttpStatusCode.Unauthorized || responsePut.StatusCode == HttpStatusCode.Forbidden)
	throw new UnauthorizedAccessException("iCloud authentication failed.");
	_logger.LogWarning("Failed to sync event '{Subject}' UID {Uid}: {Status} - {Reason}", dto.Subject, uid, responsePut.StatusCode, responsePut.ReasonPhrase);
	await RetryRequestAsync(client, requestPut, token);
}
}

if (total > 0)
_tray.UpdateText($"Updating... {total}/{total} (100%)");
}

private async Task RetryRequestAsync(HttpClient client, HttpRequestMessage original, CancellationToken token)
{
	await Task.Delay(5000, token);
	using var request = new HttpRequestMessage(original.Method, original.RequestUri);

	if (original.Content is StringContent sc)
	{
		var body = await sc.ReadAsStringAsync();
		request.Content = new StringContent(body, Encoding.UTF8, sc.Headers.ContentType?.MediaType ?? "text/plain");
	}

var retryResponse = await client.SendAsync(request, token);
if (!retryResponse.IsSuccessStatusCode)
_logger.LogError("Retry failed for {Method} {Url}: {Status} - {Reason}", original.Method, original.RequestUri, retryResponse.StatusCode, retryResponse.ReasonPhrase);
}
}
