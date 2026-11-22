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
			response.Content.Headers.Contains("Content-Range"))
		{
			throw new CalDavDataException("ListingIncomplete", response.StatusCode);
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
		var memberNames = new HashSet<string>(StringComparer.Ordinal);
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

			var properties = ReadResourceProperties(entry, dav);
			if (!properties.TryGetValue(dav + "resourcetype", out var resourceType) || resourceType == null ||
				resourceType.Nodes().OfType<XText>().Any(text => !string.IsNullOrWhiteSpace(text.Value)))
			{
				throw new CalDavDataException("ListingResourceTypeUnknown");
			}
			var isCollection = resourceType.Element(dav + "collection") != null;
			var path = uri.AbsolutePath;
			if (string.Equals(path.TrimEnd('/'), calendarUri.AbsolutePath.TrimEnd('/'), StringComparison.Ordinal))
			{
				if (!isCollection)
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
			if (!memberNames.Add(name))
			{
				throw new CalDavDataException("ListingDuplicate");
			}
			if (isCollection)
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
			if (!properties.TryGetValue(dav + "getetag", out var tag) || tag == null || tag.HasElements ||
				!EntityTagHeaderValue.TryParse(tag.Value, out var etag) || !IsStrongTag(etag))
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

	private static Dictionary<XName, XElement?> ReadResourceProperties(XElement entry, XNamespace dav)
	{
		var properties = new Dictionary<XName, XElement?>();
		foreach (var propstat in entry.Elements(dav + "propstat"))
		{
			if (propstat.Elements(dav + "status").ToArray() is not [var status] || status.HasElements ||
				propstat.Elements(dav + "prop").ToArray() is not [var prop] ||
				status.Value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries) is not [var protocol, var code, ..] ||
				!protocol.StartsWith("HTTP/", StringComparison.Ordinal) ||
				code.Length != 3 || code[0] is < '2' or > '5' || !code.All(char.IsAsciiDigit))
			{
				throw new CalDavDataException("ListingResourceInvalid");
			}
			foreach (var property in prop.Elements())
			{
				if ((property.Name == dav + "resourcetype" || property.Name == dav + "getetag") &&
					!properties.TryAdd(property.Name, code == "200" ? property : null))
				{
					throw new CalDavDataException("ListingDuplicate");
				}
			}
		}
		return properties;
	}

	private static bool IsStrongTag(EntityTagHeaderValue? value) => value is { IsWeak: false } && value.Tag != "*";

	private static EntityTagHeaderValue? GetStrongResponseTag(HttpResponseMessage response) =>
		response.Headers.TryGetValues("ETag", out var values) && values.Take(2).ToArray() is [var value] &&
		EntityTagHeaderValue.TryParse(value, out var tag) && IsStrongTag(tag) ? tag : null;

	private async Task<CalendarEvent> ReadRemoteEventAsync(HttpClient client, Uri uri, string uid,
		EntityTagHeaderValue? expectedTag, bool verification, CancellationToken token)
	{
		using var response = await SendCalDavAsync(client, () =>
		{
			var request = new HttpRequestMessage(HttpMethod.Get, uri);
			request.Headers.CacheControl = new CacheControlHeaderValue { NoCache = true };
			if (expectedTag != null)
			{
				request.Headers.IfMatch.Add(expectedTag);
			}
			return request;
		}, verification ? "event verification read" : "event comparison read", token, retryVerificationAccess: verification).ConfigureAwait(false);
		if (response.StatusCode != HttpStatusCode.OK || response.Content.Headers.Contains("Content-Range"))
		{
			throw new CalDavDataException("EventReadIncomplete", response.StatusCode);
		}
		var tag = GetStrongResponseTag(response);
		if (tag == null)
		{
			throw new CalDavDataException("EventETagInvalid", response.StatusCode);
		}
		if (expectedTag != null && !expectedTag.Equals(tag))
		{
			throw new CalDavDataException("EventRevisionChanged", response.StatusCode);
		}

		var ics = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
		if (!HasCompleteEventCalendar(ics, token))
		{
			throw new CalDavDataException("EventMalformed", response.StatusCode);
		}
		Calendar? calendar;
		try
		{
			calendar = Calendar.Load(ics);
		}
		catch (Exception ex) when (ex is not OperationCanceledException)
		{
			throw new CalDavDataException("EventMalformed", response.StatusCode);
		}
		if (calendar?.Events.Count != 1 || string.IsNullOrWhiteSpace(calendar.Events.First().Uid))
		{
			throw new CalDavDataException("EventMalformed", response.StatusCode);
		}
		if (!string.Equals(calendar.Events.First().Uid, uid, StringComparison.Ordinal))
		{
			throw new CalDavDataException("EventIdentityMismatch", response.StatusCode);
		}
		return calendar.Events.First();
	}

	private static bool HasCompleteEventCalendar(string ics, CancellationToken token)
	{
		using var lines = ReadCalendarLines(ics, token).GetEnumerator();
		if (!lines.MoveNext() || !string.Equals(lines.Current, "BEGIN:VCALENDAR", StringComparison.OrdinalIgnoreCase))
		{
			return false;
		}

		var components = new Stack<string>();
		components.Push("VCALENDAR");
		var eventSeen = false;
		var uidSeen = false;
		while (lines.MoveNext())
		{
			var line = lines.Current;
			if (!components.TryPeek(out var parent))
			{
				if (line.Length != 0)
				{
					return false;
				}
			}
			else if (line.StartsWith("BEGIN:", StringComparison.OrdinalIgnoreCase))
			{
				var component = line[6..].ToUpperInvariant();
				var allowed = parent switch
				{
					"VCALENDAR" => component == "VTIMEZONE" || (component == "VEVENT" && !eventSeen),
					"VEVENT" => component == "VALARM",
					"VTIMEZONE" => component is "STANDARD" or "DAYLIGHT",
					_ => false
				};
				if (!allowed)
				{
					return false;
				}
				eventSeen |= component == "VEVENT";
				components.Push(component);
			}
			else if (line.StartsWith("END:", StringComparison.OrdinalIgnoreCase))
			{
				if (!line.AsSpan(4).Equals(parent, StringComparison.OrdinalIgnoreCase))
				{
					return false;
				}
				components.Pop();
			}
			else if (parent == "VEVENT" && (line.StartsWith("UID:", StringComparison.OrdinalIgnoreCase) ||
				line.StartsWith("UID;", StringComparison.OrdinalIgnoreCase)))
			{
				if (uidSeen)
				{
					return false;
				}
				uidSeen = true;
			}
		}

		return components.Count == 0 && eventSeen && uidSeen;
	}

	private static IEnumerable<string> ReadCalendarLines(string ics, CancellationToken token)
	{
		using var reader = new StringReader(ics);
		var nextLine = reader.ReadLine();
		while (nextLine != null)
		{
			token.ThrowIfCancellationRequested();
			var line = nextLine;
			nextLine = reader.ReadLine();
			if (nextLine is not [' ' or '\t', ..])
			{
				yield return line;
				continue;
			}

			var unfolded = new StringBuilder(line);
			do
			{
				token.ThrowIfCancellationRequested();
				unfolded.Append(nextLine.AsSpan(1));
				nextLine = reader.ReadLine();
			} while (nextLine is [' ' or '\t', ..]);
			yield return unfolded.ToString();
		}
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

internal sealed class CalDavDataException(string reason, HttpStatusCode? statusCode = null) : Exception(reason)
{
	internal string Reason { get; } = reason;
	internal HttpStatusCode? StatusCode { get; } = statusCode;
}
