using Ical.Net;
using Ical.Net.CalendarComponents;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace CalendarSync;

public partial class CalendarSyncService
{
	internal sealed record RemoteResource(string Name, Uri Uri, EntityTagHeaderValue ETag);

	private async Task<Dictionary<string, RemoteResource>> GetICloudEventsAsync(HttpClient client, Uri calendarUri, string? sourceId, CancellationToken token)
	{
		const string requestBody = "<d:propfind xmlns:d=\"DAV:\"><d:prop><d:getetag/><d:resourcetype/></d:prop></d:propfind>";
		using var response = await SendCalDavAsync(client, () =>
		{
			var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), calendarUri)
			{
				Content = new StringContent(requestBody, Encoding.UTF8, "application/xml")
			};
			request.Headers.Add("Depth", "1");
			return request;
		}, "calendar listing", token).ConfigureAwait(false);
		return await ReadICloudEventsAsync(response, calendarUri, sourceId, token).ConfigureAwait(false);
	}

	internal static async Task<Dictionary<string, RemoteResource>> ReadICloudEventsAsync(
		HttpResponseMessage response, Uri calendarUri, string? sourceId, CancellationToken token)
	{
		if (response.StatusCode != HttpStatusCode.MultiStatus || response.Headers.Contains("Link") ||
			response.Content.Headers.ContentRange != null)
		{
			throw new CalDavDataException("ListingIncomplete");
		}

		XDocument document;
		try
		{
			var content = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
			using var reader = XmlReader.Create(new StringReader(content), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
			document = XDocument.Load(reader);
		}
		catch (XmlException)
		{
			throw new CalDavDataException("ListingMalformed");
		}

		XNamespace dav = "DAV:";
		var root = document.Root;
		if (root?.Name != dav + "multistatus" || root.Descendants(dav + "error").Any() ||
			root.Elements().Any(element => element.Name != dav + "response" && element.Name != dav + "responsedescription"))
		{
			throw new CalDavDataException("ListingIncomplete");
		}

		var resources = new Dictionary<string, RemoteResource>(StringComparer.OrdinalIgnoreCase);
		var collectionSeen = false;
		foreach (var entry in root.Elements(dav + "response"))
		{
			token.ThrowIfCancellationRequested();
			var hrefs = entry.Elements(dav + "href").ToArray();
			if (hrefs.Length != 1 || string.IsNullOrWhiteSpace(hrefs[0].Value) ||
				!Uri.TryCreate(calendarUri, hrefs[0].Value, out var uri) ||
				uri.Scheme != calendarUri.Scheme || !uri.Authority.Equals(calendarUri.Authority, StringComparison.OrdinalIgnoreCase) ||
				uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0 ||
				entry.Element(dav + "status") != null)
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}

			var properties = entry.Elements(dav + "propstat")
				.Where(propstat => IsDavSuccess(propstat.Element(dav + "status")?.Value))
				.SelectMany(propstat => propstat.Elements(dav + "prop")).ToArray();
			if (properties.Length == 0)
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}
			var path = uri.AbsolutePath;
			if (string.Equals(path.TrimEnd('/'), calendarUri.AbsolutePath.TrimEnd('/'), StringComparison.Ordinal))
			{
				if (!properties.Elements(dav + "resourcetype").Elements(dav + "collection").Any())
				{
					throw new CalDavDataException("ListingIncomplete");
				}
				if (collectionSeen)
				{
					throw new CalDavDataException("ListingDuplicate");
				}
				collectionSeen = true;
				continue;
			}

			if (!path.StartsWith(calendarUri.AbsolutePath, StringComparison.Ordinal))
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}
			var segment = path[calendarUri.AbsolutePath.Length..];
			var name = Uri.UnescapeDataString(segment.TrimEnd('/'));
			if (name.Length == 0 || name.Contains('/') || name.Contains('\\') || name is "." or "..")
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}
			if (properties.Elements(dav + "resourcetype").Elements(dav + "collection").Any())
			{
				continue;
			}
			if (segment.EndsWith('/'))
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}
			if (!name.EndsWith(".ics", StringComparison.OrdinalIgnoreCase))
			{
				continue;
			}

			var uid = name[..^4];
			if (sourceId != null && !CalendarRules.IsManagedUid(sourceId, uid))
			{
				continue;
			}
			var tags = properties.Elements(dav + "getetag").ToArray();
			if (tags.Length != 1 || !EntityTagHeaderValue.TryParse(tags[0].Value, out var etag) || !IsStrongTag(etag))
			{
				throw new CalDavDataException("ListingETagInvalid");
			}
			if (!resources.TryAdd(uid, new RemoteResource(uid, uri, etag!)))
			{
				throw new CalDavDataException("ListingDuplicate");
			}
		}
		if (!collectionSeen)
		{
			throw new CalDavDataException("ListingIncomplete");
		}
		return resources;
	}

	private static bool IsDavSuccess(string? status) =>
		status?.Split(' ', StringSplitOptions.RemoveEmptyEntries) is [var protocol, "200", ..] &&
		protocol.StartsWith("HTTP/", StringComparison.Ordinal);

	private static bool IsStrongTag(EntityTagHeaderValue? value) => value is { IsWeak: false } && value.Tag != "*";

	private async Task<CalendarEvent> ReadRemoteEventAsync(HttpClient client, Uri uri, string uid,
		EntityTagHeaderValue? expectedTag, bool verification, CancellationToken token)
	{
		using var response = await SendCalDavAsync(client, () =>
		{
			var request = new HttpRequestMessage(HttpMethod.Get, uri);
			if (expectedTag != null)
			{
				request.Headers.IfMatch.Add(expectedTag);
			}
			return request;
		}, verification ? "event verification read" : "event comparison read", token, retryVerificationAccess: verification).ConfigureAwait(false);
		if (response.StatusCode != HttpStatusCode.OK || !IsStrongTag(response.Headers.ETag))
		{
			throw new CalDavDataException("EventReadInvalid");
		}
		if (expectedTag != null && !expectedTag.Equals(response.Headers.ETag))
		{
			throw new CalDavDataException("EventRevisionChanged");
		}

		var ics = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
		Calendar? calendar;
		try
		{
			calendar = Calendar.Load(ics);
		}
		catch (Exception ex) when (ex is not OperationCanceledException)
		{
			throw new CalDavDataException("EventMalformed");
		}
		if (calendar?.Events.Count != 1 || !string.Equals(calendar.Events.First().Uid, uid, StringComparison.Ordinal))
		{
			throw new CalDavDataException("EventIdentityMismatch");
		}
		return calendar.Events.First();
	}

	private HttpClient CreateHttpClient()
	{
		var client = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false })
		{
			Timeout = TimeSpan.FromSeconds(90),
			MaxResponseContentBufferSize = 32 * 1024 * 1024
		};
		var credentials = Encoding.UTF8.GetBytes($"{_config.ICloudUser}:{_config.ICloudPassword}");
		client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));
		client.DefaultRequestHeaders.Add("User-Agent", "CalendarSyncService");
		return client;
	}
}

internal sealed class CalDavDataException(string reason) : Exception(reason)
{
	internal string Reason { get; } = reason;
}
