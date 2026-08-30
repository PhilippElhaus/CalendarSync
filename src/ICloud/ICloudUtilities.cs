using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml;
using System.Xml.Linq;


namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task<Dictionary<string, string>> GetICloudEventsAsync(HttpClient client, string calendarUrl, bool filterBySource, CancellationToken token)
	{
		var events = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		const string requestBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><d:propfind xmlns:d=\"DAV:\" xmlns:cs=\"http://calendarserver.org/ns/\"><d:prop><d:getetag/><cs:getctag/></d:prop></d:propfind>";

		try
		{
			using var response = await SendCalDavAsync(
				client,
				() =>
				{
					var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), calendarUrl)
					{
						Content = new StringContent(requestBody, Encoding.UTF8, "application/xml")
					};
					request.Headers.Add("Depth", "1");
					return request;
				},
				"calendar listing",
				token).ConfigureAwait(false);
			var content = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
			var document = XDocument.Parse(content);

			XNamespace dav = "DAV:";
			foreach (var responseElement in document.Descendants(dav + "response"))
			{
				var href = responseElement.Element(dav + "href")?.Value;
				var propStat = responseElement.Element(dav + "propstat");
				var prop = propStat?.Element(dav + "prop");
				var etag = prop?.Element(dav + "getetag")?.Value ?? string.Empty;

				if (string.IsNullOrEmpty(href) || !href.EndsWith(".ics", StringComparison.OrdinalIgnoreCase))
				{
					continue;
				}

				var uid = href.Trim('/').Split('/').Last().Replace(".ics", string.Empty, StringComparison.OrdinalIgnoreCase);
				if (filterBySource && !IsManagedUid(uid))
				{
					continue;
				}

				events[uid] = etag;
			}
		}
		catch (HttpRequestException ex)
		{
			_logger.LogError(ex, "Failed to retrieve iCloud PROPFIND response.");
			EventRecorder.WriteEntry("iCloud PROPFIND failed", EventLogEntryType.Error);
			throw;
		}
		catch (XmlException ex)
		{
			_logger.LogError(ex, "Failed to parse iCloud PROPFIND response.");
			EventRecorder.WriteEntry("iCloud response parse failed", EventLogEntryType.Error);
			throw new InvalidOperationException("iCloud PROPFIND response was not valid XML.", ex);
		}

		_logger.LogInformation("Parsed {Count} events from PROPFIND response.", events.Count);
		return events;
	}

	private async Task<string> GetICloudEventDescriptionAsync(HttpClient client, string eventUrl, CancellationToken token)
	{
		using var response = await SendCalDavAsync(
			client,
			() => new HttpRequestMessage(HttpMethod.Get, eventUrl),
			"event description read",
			token).ConfigureAwait(false);

		var ics = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
		var calendar = Calendar.Load(ics);
		var calendarEvent = calendar?.Events?.FirstOrDefault()
			?? throw new InvalidDataException("The existing iCloud event contained no VEVENT while preserving its description.");
		return calendarEvent.Description ?? string.Empty;
	}

	private CalendarEvent CreateCalendarEvent(OutlookEventDto appt, string uid)
	{
		return CalendarRules.CreateCalendarEvent(appt, uid, _tag);
	}

	private HttpClient CreateHttpClient()
	{
		var client = new HttpClient
		{
			Timeout = TimeSpan.FromSeconds(90)
		};
		var credentials = Encoding.UTF8.GetBytes($"{_config.ICloudUser}:{_config.ICloudPassword}");
		client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));
		client.DefaultRequestHeaders.Add("User-Agent", "CalendarSyncService");
		return client;
	}

	private bool IsManagedUid(string? uid)
	{
		return CalendarRules.IsManagedUid(_sourceId, uid);
	}
}
