using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Xml.Linq;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Microsoft.Extensions.Logging;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task<Dictionary<string, string>> GetICloudEventsAsync(HttpClient client, string calendarUrl, bool filterBySource)
	{
		var events = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		var requestBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><d:propfind xmlns:d=\"DAV:\" xmlns:cs=\"http://calendarserver.org/ns/\"><d:prop><d:getetag/><cs:getctag/></d:prop></d:propfind>";
		var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), calendarUrl)
		{
			Content = new StringContent(requestBody, Encoding.UTF8, "application/xml")
		};
	request.Headers.Add("Depth", "1");

	try
	{
		var response = await client.SendAsync(request);
		response.EnsureSuccessStatusCode();
		var content = await response.Content.ReadAsStringAsync();
		var document = XDocument.Parse(content);

		XNamespace dav = "DAV:";
		foreach (var responseElement in document.Descendants(dav + "response"))
		{
			var href = responseElement.Element(dav + "href")?.Value;
			var propStat = responseElement.Element(dav + "propstat");
			var prop = propStat?.Element(dav + "prop");
			var etag = prop?.Element(dav + "getetag")?.Value ?? string.Empty;

			if (string.IsNullOrEmpty(href) || !href.EndsWith(".ics", StringComparison.OrdinalIgnoreCase))
			continue;

			var uid = href.Trim('/').Split('/').Last().Replace(".ics", string.Empty, StringComparison.OrdinalIgnoreCase);
			if (filterBySource && !IsManagedUid(uid))
			continue;

			events[uid] = etag;
		}
}
catch (Exception ex)
{
	_logger.LogError(ex, "Failed to parse PROPFIND response.");
	EventRecorder.WriteEntry("iCloud response parse failed", EventLogEntryType.Error);
}

_logger.LogInformation("Parsed {Count} events from PROPFIND response.", events.Count);
return events;
}

private CalendarEvent CreateCalendarEvent(OutlookEventDto appt, string uid)
{
	var summary = appt.Subject ?? "No Subject";
	if (!string.IsNullOrEmpty(_tag))
	summary = $"[{_tag}] {summary}";

	CalDateTime start;
	CalDateTime end;

	var span = appt.EndLocal - appt.StartLocal;
	var isAllDay = appt.StartLocal.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
	(appt.EndLocal.TimeOfDay == TimeSpan.Zero || appt.EndLocal.TimeOfDay >= new TimeSpan(23, 59, 0));

	if (isAllDay)
	{
		var startDate = appt.StartLocal.Date;
		var endLocalDate = appt.EndLocal.TimeOfDay == TimeSpan.Zero ? appt.EndLocal.Date : appt.EndLocal.Date.AddDays(1);
		start = new CalDateTime(startDate.Year, startDate.Month, startDate.Day);
		end = new CalDateTime(endLocalDate.Year, endLocalDate.Month, endLocalDate.Day);
	}
else
{
	start = new CalDateTime(appt.StartUtc);
	end = new CalDateTime(appt.EndUtc);
}

var calEvent = new CalendarEvent
{
	Summary = summary,
	Start = start,
	End = end,
	Location = appt.Location ?? string.Empty,
	Uid = uid,
	Description = appt.Body ?? string.Empty,
};

		if (!isAllDay)
		{
			calEvent.Alarms.Add(new Alarm { Action = "DISPLAY", Description = "Reminder", Trigger = new Trigger("-PT10M") });
			if (_config.IncludeSecondReminder)
			{
				calEvent.Alarms.Add(new Alarm { Action = "DISPLAY", Description = "Reminder", Trigger = new Trigger("-PT3M") });
			}
		}

		return calEvent;
}

private HttpClient CreateHttpClient()
{
	var client = new HttpClient();
	client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
	"Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_config.ICloudUser}:{_config.ICloudPassword}")));
	client.DefaultRequestHeaders.Add("User-Agent", "CalendarSyncService");
	return client;
}

private bool IsManagedUid(string? uid)
{
	if (string.IsNullOrWhiteSpace(uid))
	return false;

	var normalized = uid.Trim();
	var prefixes = new List<string>();

	if (!string.IsNullOrEmpty(_sourceId))
	prefixes.Add($"{_sourceId}-outlook-");

	prefixes.Add("-outlook-");
	prefixes.Add("outlook-");

	foreach (var prefix in prefixes)
	{
		if (normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
		return true;
	}

return false;
}
}
