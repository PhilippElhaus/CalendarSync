using System.Net;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task WipeICloudCalendarAsync(HttpClient client, Uri calendarUri, CycleMetrics metrics, CancellationToken token)
	{
		metrics.Stage = "WipeListing";
		var resources = await GetICloudEventsAsync(client, calendarUri, null, token).ConfigureAwait(false);
		metrics.WipeListed = resources.Count;
		metrics.Stage = "WipeDelete";
		_tray?.SetDeleting();
		foreach (var resource in resources.Values)
		{
			_tray?.UpdateText($"Deleting... {metrics.WipeDeleted}/{resources.Count}");
			await Task.Delay(300, token).ConfigureAwait(false);
			await DeleteResourceAsync(client, resource, "calendar wipe deletion", token).ConfigureAwait(false);
			metrics.WipeDeleted++;
		}

		if (metrics.WipeDeleted > 0)
		{
			metrics.Stage = "WipeRecheckWait";
			await Task.Delay(TimeSpan.FromSeconds(30), token).ConfigureAwait(false);
		}
	}

	private async Task DeleteResourceAsync(HttpClient client, RemoteResource resource, string operation, CancellationToken token)
	{
		using var response = await SendCalDavAsync(client, () =>
		{
			var request = new HttpRequestMessage(HttpMethod.Delete, resource.Uri);
			request.Headers.IfMatch.Add(resource.ETag);
			return request;
		}, operation, token).ConfigureAwait(false);
		if (response.StatusCode is not (HttpStatusCode.NoContent or HttpStatusCode.OK))
		{
			throw new CalDavDataException("DeleteNotConfirmed");
		}
	}
}
