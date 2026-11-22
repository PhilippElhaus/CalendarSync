using Ical.Net;
using Ical.Net.Serialization;
using System.Text;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task SyncWithICloudAsync(HttpClient client, Uri calendarUri, Dictionary<string, OutlookEventDto> outlookEvents,
		Dictionary<string, RemoteResource> remote, CycleMetrics metrics, CancellationToken token)
	{
		metrics.Listed = remote.Count;
		_tray?.SetUpdating();

		var done = 0;
		foreach (var (uid, dto) in outlookEvents)
		{
			token.ThrowIfCancellationRequested();
			_tray?.UpdateText($"Updating... {++done}/{outlookEvents.Count}");
			metrics.Stage = "Compare";
			remote.TryGetValue(uid, out var resource);
			var eventUri = resource?.Uri ?? new Uri(calendarUri, $"{Uri.EscapeDataString(uid)}.ics");
			var desired = CalendarRules.CreateCalendarEvent(dto, uid, _tag);
			if (resource != null)
			{
				if (!string.Equals(resource.Name, uid, StringComparison.Ordinal))
				{
					throw new CalDavDataException("EventNameMismatch");
				}
				var existing = await ReadRemoteEventAsync(client, eventUri, uid, resource.ETag, false, token).ConfigureAwait(false);
				if (!dto.BodyWasRead)
				{
					desired.Description = existing.Description ?? string.Empty;
				}
				if (MatchesEvent(desired, existing))
				{
					metrics.Unchanged++;
					continue;
				}
			}

			metrics.Stage = "Upsert";
			var calendar = new Calendar { Events = { desired } };
			var payload = new CalendarSerializer().SerializeToString(calendar);
			if (string.IsNullOrEmpty(payload))
			{
				throw new CalDavDataException("EventSerializationFailed");
			}
			using var response = await SendMutationAsync(client, () =>
			{
				var request = new HttpRequestMessage(HttpMethod.Put, eventUri)
				{
					Content = new StringContent(payload, Encoding.UTF8, "text/calendar")
				};
				if (resource == null)
				{
					request.Headers.IfNoneMatch.Add(System.Net.Http.Headers.EntityTagHeaderValue.Any);
				}
				else
				{
					request.Headers.IfMatch.Add(resource.ETag);
				}
				return request;
			}, "event upsert", metrics.Writes, token, isUpsert: true).ConfigureAwait(false);

			metrics.Stage = "Verify";
			var tag = GetStrongResponseTag(response);
			var verified = await ReadRemoteEventAsync(client, eventUri, uid, tag, true, token).ConfigureAwait(false);
			if (!MatchesEvent(desired, verified))
			{
				throw new CalDavDataException("EventContentMismatch");
			}
			metrics.Verified++;
		}

		metrics.Stage = "StaleOwnershipRead";
		var stale = CalendarRules.GetStaleManagedUids(_sourceId, remote.Keys, outlookEvents.Keys);
		metrics.StaleCandidates = stale.Count;
		if (stale.Count > 0)
		{
			_tray?.SetDeleting();
		}
		var ownedStale = new List<RemoteResource>();
		foreach (var uid in stale)
		{
			var resource = remote[uid];
			try
			{
				await ReadRemoteEventAsync(client, resource.Uri, uid, resource.ETag, false, token).ConfigureAwait(false);
				ownedStale.Add(resource);
			}
			catch (CalDavDataException ex) when (ex.Reason == "EventIdentityMismatch")
			{
				metrics.OwnershipSkipped++;
			}
		}
		metrics.Stage = "StaleDelete";
		foreach (var resource in ownedStale)
		{
			await DeleteResourceAsync(client, resource, "stale event deletion", metrics.Deletes, token).ConfigureAwait(false);
		}
	}
}
