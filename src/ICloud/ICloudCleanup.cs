namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task<Dictionary<string, RemoteResource>> WipeICloudCalendarAsync(HttpClient client, Uri calendarUri, CycleMetrics metrics, CancellationToken token)
	{
		metrics.Stage = "WipeListing";
		var resources = await GetICloudEventsAsync(client, calendarUri, null, token).ConfigureAwait(false);
		metrics.WipeListed = resources.Count;
		metrics.Stage = "WipeDelete";
		_tray?.SetDeleting();
		foreach (var resource in resources.Values)
		{
			_tray?.UpdateText($"Deleting... {metrics.WipeDeletes.Confirmed}/{resources.Count}");
			await _wipeDelay(TimeSpan.FromMilliseconds(300), token).ConfigureAwait(false);
			await DeleteResourceAsync(client, resource, "calendar wipe deletion", metrics.WipeDeletes, token).ConfigureAwait(false);
		}

		if (metrics.WipeDeletes.Confirmed > 0)
		{
			metrics.Stage = "WipeRecheckWait";
			await _wipeDelay(TimeSpan.FromSeconds(30), token).ConfigureAwait(false);
		}

		metrics.Stage = "WipeRecheck";
		var remaining = await GetICloudEventsAsync(client, calendarUri, null, token).ConfigureAwait(false);
		metrics.WipeRemaining = remaining.Count;
		if (remaining.Count > 0)
		{
			throw new CalDavDataException("WipeNotEmpty");
		}
		return remaining;
	}

	private async Task DeleteResourceAsync(HttpClient client, RemoteResource resource, string operation, MutationCounts counts, CancellationToken token)
	{
		using var response = await SendMutationAsync(client, () =>
		{
			var request = new HttpRequestMessage(HttpMethod.Delete, resource.Uri);
			request.Headers.IfMatch.Add(resource.ETag);
			return request;
		}, operation, counts, token).ConfigureAwait(false);
	}
}
