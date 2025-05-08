using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Ical.Net.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml.Linq;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public class CalendarSyncService : BackgroundService
{
	private readonly SyncConfig _config;
	private readonly ILogger<CalendarSyncService> _logger;
	private static bool _isFirstRun = true;
	private readonly TimeSpan _initialWait;
	private readonly TimeSpan _syncInterval;
	private readonly HashSet<DateTime> _brokenExceptionDates = new();


	public CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger)
	{
		_config = config ?? throw new ArgumentNullException(nameof(config));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_initialWait = TimeSpan.FromSeconds(_config.InitialWaitSeconds);
		_syncInterval = TimeSpan.FromMinutes(_config.SyncIntervalMinutes);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{

		_logger.LogInformation("Calendar Sync Service started.");

		_logger.LogInformation("Initial wait for {InitialWait} seconds before starting sync.", _initialWait.TotalSeconds);
		await Task.Delay(_initialWait, stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				await PerformSyncAsync();
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Unexpected error during sync. Continuing to next cycle.");
			}
			_logger.LogDebug("Waiting for next sync cycle.");
			await Task.Delay(_syncInterval, stoppingToken);
		}
		_logger.LogInformation("Calendar Sync Service stopped.");
	}

	private async Task PerformSyncAsync()
	{
		_logger.LogInformation("Starting sync at {Time}", DateTime.Now);

		Outlook.Application outlookApp = null;
		Outlook.NameSpace outlookNs = null;
		Outlook.MAPIFolder calendar = null;
		Outlook.Items items = null;

		int retryCount = 0;
		const int maxRetries = 5;
		bool connected = false;

		while (retryCount < maxRetries && !connected)
		{
			try
			{
				_logger.LogDebug("Attempting to create Outlook.Application instance.");
				outlookApp = new Outlook.Application();
				_logger.LogDebug("Getting Outlook namespace.");
				outlookNs = outlookApp.GetNamespace("MAPI");
				_logger.LogDebug("Accessing calendar folder.");
				calendar = outlookNs.GetDefaultFolder(Outlook.OlDefaultFolders.olFolderCalendar);
				_logger.LogDebug("Retrieving calendar items.");
				items = calendar.Items;
				_logger.LogInformation("Successfully connected to Outlook.");
				connected = true;
			}
			catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
			{
				retryCount++;
				_logger.LogWarning(ex, $"Failed to connect to Outlook (CO_E_SERVER_EXEC_FAILURE), retry {retryCount}/{maxRetries}.");
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
				if (retryCount == maxRetries)
				{
					_logger.LogError("Max retries reached for Outlook connection. Skipping this sync cycle.");
					return;
				}
				_logger.LogDebug("Waiting 10 seconds before retry.");
				await Task.Delay(10000);
			}
			catch (Exception ex)
			{
				retryCount++;
				_logger.LogWarning(ex, "Unexpected error connecting to Outlook, retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
				if (retryCount == maxRetries)
				{
					_logger.LogError("Max retries reached for Outlook connection. Skipping this sync cycle.");
					return;
				}
				_logger.LogDebug("Waiting 10 seconds before retry.");
				await Task.Delay(10000);
			}
		}

		if (!connected)
		{
			_logger.LogDebug("No connection established, exiting PerformSyncAsync.");
			return;
		}

		try
		{
			items.IncludeRecurrences = true;
			items.Sort("[Start]");

			DateTime start = DateTime.Today.AddDays(-30);
			DateTime end = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);

			var allItems = new List<Outlook.AppointmentItem>();
			int count = 0;

			foreach (object item in items)
			{
				if (count++ > 1000)
				{
					_logger.LogWarning("Aborting calendar item scan after 1000 items to prevent hangs.");
					break;
				}

				try
				{
					if (item is Outlook.AppointmentItem appt)
					{
						allItems.Add(appt); 
					}
				}
				catch (Exception ex)
				{
					_logger.LogDebug(ex, "Skipping calendar item due to exception.");
				}
			}

			_logger.LogInformation("Collected {Count} Outlook items after manual date filter.", allItems.Count);

			var outlookEvents = GetOutlookEventsFromList(allItems);

			_logger.LogInformation("Expanded to {Count} atomic Outlook events.", outlookEvents.Count);

			using var client = CreateHttpClient();
			string calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";

			if (_isFirstRun)
			{
				_logger.LogInformation("First run detected, initiating wipe.");
				await WipeICloudCalendarAsync(client, calendarUrl);
				_isFirstRun = false;
			}

			await SyncWithICloudAsync(client, outlookEvents);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error during sync processing. Skipping this cycle.");
		}
		finally
		{
			_logger.LogDebug("Cleaning up Outlook COM objects.");
			CleanupOutlook(outlookApp, outlookNs, calendar, items);
		}

		_logger.LogInformation("Sync completed at {Time}", DateTime.Now);
	}

	private async Task WipeICloudCalendarAsync(HttpClient client, string calendarUrl)
	{
		_logger.LogInformation("Wiping entire iCloud calendar (past and future events).");
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl);
		_logger.LogInformation("Found {Count} existing iCloud events to delete.", iCloudEvents.Count);

		foreach (var iCloudUid in iCloudEvents.Keys)
		{
			string eventUrl = $"{calendarUrl}{iCloudUid}.ics";
			var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
			await Task.Delay(500);
			try
			{
				var deleteResponse = await client.SendAsync(deleteRequest);
				if (deleteResponse.IsSuccessStatusCode)
				{
					_logger.LogInformation("Deleted iCloud event with UID {Uid}", iCloudUid);
				}
				else
				{
					_logger.LogWarning("Failed to delete iCloud event UID {Uid}: {Status} - {Reason}", iCloudUid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Exception while deleting iCloud event UID {Uid}", iCloudUid);
				await Task.Delay(5000);
			}
		}

		_logger.LogInformation("Finished full iCloud calendar wipe. Waiting 2 minutes for cache to clear.");
		await Task.Delay(TimeSpan.FromMinutes(2));
	}

	private Dictionary<string, Outlook.AppointmentItem> GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts)
	{
		var events = new Dictionary<string, Outlook.AppointmentItem>();
		DateTime syncStart = DateTime.Today.AddDays(-30);
		DateTime syncEnd = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);

		foreach (var appt in appts)
		{
			if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
				continue;

			if (!appt.IsRecurring)
			{
				string uid = $"outlook-{appt.GlobalAppointmentID}-{appt.Start:yyyyMMddTHHmmss}";
				_logger.LogDebug("Adding single event UID: {Uid} Start: {Start}", uid, appt.Start);
				events[uid] = appt;
				continue;
			}

			Outlook.RecurrencePattern pattern = null;
			try
			{
				pattern = appt.GetRecurrencePattern();
			}
			catch (COMException ex)
			{
				_logger.LogDebug(ex, "Failed to get recurrence pattern for recurring appointment. Skipping.");
				continue;
			}

			if (pattern == null)
				continue;

			DateTime instanceTime = syncStart;
			int instanceLimit = 1000;
			int instanceCount = 0;

			while (instanceTime < syncEnd && instanceCount++ < instanceLimit)
			{
				_logger.LogDebug("Checking recurrence on {Date}", instanceTime);

				Outlook.AppointmentItem instance = null;
				try
				{
					instance = pattern.GetOccurrence(instanceTime);
				}
				catch
				{
					instanceTime = instanceTime.AddDays(1);
					continue;
				}

				if (instance != null &&
					instance.Start < syncEnd && instance.End > syncStart &&
					instance.MeetingStatus != Outlook.OlMeetingStatus.olMeetingCanceled)
				{
					string instanceUid = $"outlook-{appt.GlobalAppointmentID}-{instance.Start:yyyyMMddTHHmmss}";
					_logger.LogDebug("Adding instance UID: {Uid} Start: {Start}", instanceUid, instance.Start);
					events[instanceUid] = instance;
				}

				instanceTime = instanceTime.AddDays(1);
			}

			Marshal.ReleaseComObject(pattern);
		}

		return events;
	}
	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, Outlook.AppointmentItem> outlookEvents)
	{
		string calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl); // UID -> etag (unused)

		_logger.LogInformation("Found {Count} iCloud events before sync.", iCloudEvents.Count);

		foreach (var (uid, appt) in outlookEvents)
		{
			if (appt == null)
				continue;

			var calEvent = CreateCalendarEvent(appt, uid);
			var calendar = new Calendar { Events = { calEvent } };
			var serializer = new CalendarSerializer();
			string newIcs = serializer.SerializeToString(calendar);

			string eventUrl = $"{calendarUrl}{uid}.ics";

			bool needsUpdate = true;

			if (iCloudEvents.ContainsKey(uid))
			{
				// No actual diffing here; always replace if exists (safe fallback in flat mode)
				needsUpdate = true;
			}

			if (!needsUpdate)
			{
				_logger.LogDebug("Skipping unchanged event UID {Uid}", uid);
				continue;
			}

			var requestPut = new HttpRequestMessage(HttpMethod.Put, eventUrl)
			{
				Content = new StringContent(newIcs, Encoding.UTF8, "text/calendar")
			};

			var responsePut = await client.SendAsync(requestPut);
			if (responsePut.IsSuccessStatusCode)
			{
				_logger.LogInformation("Synced event '{Subject}' UID {Uid}", appt.Subject, uid);
			}
			else
			{
				_logger.LogWarning("Failed to sync event '{Subject}' UID {Uid}: {Status} - {Reason}", appt.Subject, uid, responsePut.StatusCode, responsePut.ReasonPhrase);
				await RetryRequestAsync(client, requestPut);
			}
		}

		// Cleanup orphaned iCloud events not present in Outlook
		foreach (var uid in iCloudEvents.Keys)
		{
			if (!outlookEvents.ContainsKey(uid))
			{
				string eventUrl = $"{calendarUrl}{uid}.ics";
				var request = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
				var response = await client.SendAsync(request);

				if (response.IsSuccessStatusCode)
					_logger.LogInformation("Deleted orphaned iCloud event UID {Uid}", uid);
				else
					_logger.LogWarning("Failed to delete orphaned iCloud event UID {Uid}: {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
			}
		}
	}


	private async Task<Dictionary<string, string>> GetICloudEventsAsync(HttpClient client, string calendarUrl)
	{
		var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), calendarUrl)
		{
			Content = new StringContent(
				@"<?xml version=""1.0"" encoding=""utf-8"" ?>
            <d:propfind xmlns:d=""DAV:"" xmlns:c=""urn:ietf:params:xml:ns:caldav"">
                <d:prop>
                    <d:getetag />
                </d:prop>
            </d:propfind>",
				Encoding.UTF8, "application/xml")
		};
		request.Headers.Add("Depth", "1");

		_logger.LogInformation("Sending PROPFIND to {Url}", calendarUrl);
		var response = await client.SendAsync(request);
		var content = await response.Content.ReadAsStringAsync();
		

		if (!response.IsSuccessStatusCode)
		{
			_logger.LogWarning("Failed to fetch iCloud events: {Status} - {Reason}", response.StatusCode, response.ReasonPhrase);
			return new Dictionary<string, string>();
		}

		var events = new Dictionary<string, string>();
		try
		{
			var doc = XDocument.Parse(content);
			XNamespace dav = "DAV:"; 
			var hrefs = doc.Descendants(dav + "href")
						   .Where(h => h.Value.EndsWith(".ics"))
						   .Select(h => h.Value.Split('/').Last().Replace(".ics", ""));

			foreach (var uid in hrefs)
			{
				events[uid] = "";
				_logger.LogDebug("Found iCloud event UID: {Uid}", uid);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to parse PROPFIND response: {Content}", content);
		}

		_logger.LogInformation("Parsed {Count} events from PROPFIND response.", events.Count);
		return events;
	}

	private CalendarEvent CreateCalendarEvent(Outlook.AppointmentItem appt, string uid)
	{
		var calEvent = new CalendarEvent
		{
			Summary = appt.Subject ?? "No Subject",
			Start = new CalDateTime(appt.Start.ToUniversalTime()),
			End = new CalDateTime(appt.End.ToUniversalTime()),
			Location = appt.Location ?? "",
			Uid = uid,
			Description = appt.Body ?? ""
		};

		// No RRULE or RECURRENCE-ID added — all events are atomic

		// Add reminders
		var reminder = new Alarm
		{
			Action = AlarmAction.Display,
			Description = "Reminder",
			Trigger = new Trigger("-PT10M")
		};

		var secondReminder = new Alarm
		{
			Action = AlarmAction.Display,
			Description = "Reminder",
			Trigger = new Trigger("-PT3M")
		};

		calEvent.Alarms.Add(secondReminder);
		calEvent.Alarms.Add(reminder);

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

	private async Task RetryRequestAsync(HttpClient client, HttpRequestMessage request)
	{
		await Task.Delay(5000);
		var retryResponse = await client.SendAsync(request);
		if (!retryResponse.IsSuccessStatusCode)
		{
			_logger.LogError("Retry failed for {Method} {Url}: {Status} - {Reason}", request.Method, request.RequestUri, retryResponse.StatusCode, retryResponse.ReasonPhrase);
		}
	}

	private void CleanupOutlook(Outlook.Application app, Outlook.NameSpace ns, Outlook.MAPIFolder folder, Outlook.Items items)
	{
		try
		{
			if (items != null)
				Marshal.ReleaseComObject(items);
			if (folder != null)
				Marshal.ReleaseComObject(folder);
			if (ns != null)
				Marshal.ReleaseComObject(ns);
			if (app != null)
				Marshal.ReleaseComObject(app);
		}
		catch 
		{
			_logger.LogError("Unable to clean up Outlook COM objects.");
			
		}
	}
	
}