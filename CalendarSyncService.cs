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
				_logger.LogWarning(ex, $"Failed to connect to Outlook (CO_E_SERVER_EXEC_FAILURE), retry {retryCount}/{maxRetries}.", retryCount, maxRetries);
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
			items.IncludeRecurrences = false;
			items.Sort("[Start]");
			DateTime start = DateTime.Today;
			DateTime end = start.AddDays(_config.SyncDaysIntoFuture);
			string filter = $"[Start] >= '{start:g}' AND [Start] <= '{end:g}' OR [End] >= '{start:g}' AND [End] <= '{end:g}'";
			var restrictedItems = items.Restrict(filter);
			var outlookEvents = GetOutlookEvents(restrictedItems);
			_logger.LogInformation("Found {Count} Outlook events to sync.", outlookEvents.Count);

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
		_logger.LogInformation("Wiping iCloud calendar from today onward.");
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl);
		_logger.LogInformation("Found {Count} existing iCloud events to evaluate.", iCloudEvents.Count);

		foreach (var iCloudUid in iCloudEvents.Keys)
		{
			string eventUrl = $"{calendarUrl}{iCloudUid}.ics";
			var response = await client.GetAsync(eventUrl);
			if (!response.IsSuccessStatusCode)
			{
				_logger.LogWarning("Failed to fetch iCloud event UID {Uid} for evaluation: {Status}", iCloudUid, response.StatusCode);
				continue;
			}

			var content = await response.Content.ReadAsStringAsync();
			var calendar = Calendar.Load(content);
			var ev = calendar.Events.FirstOrDefault();
			if (ev == null)
			{
				_logger.LogDebug("Skipping deletion, no event found for UID {Uid}", iCloudUid);
				continue;
			}

			var end = ev.RecurrenceRules?.FirstOrDefault()?.Until ?? ev.End?.Value;
			if (end == null || end.Value.Date < DateTime.Today)
			{
				_logger.LogDebug("Skipping deletion, event UID {Uid} ends in past", iCloudUid);
				continue;
			}

			var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
			await Task.Delay(250);
			try
			{
				var deleteResponse = await client.SendAsync(deleteRequest);
				if (deleteResponse.IsSuccessStatusCode)
				{
					_logger.LogInformation("Deleted future iCloud event with UID {Uid}", iCloudUid);
				}
				else
				{
					_logger.LogWarning("Failed to delete iCloud event UID {Uid}: {Status} - {Reason}", iCloudUid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Exception while deleting iCloud event UID {Uid}", iCloudUid);
			}
		}

		_logger.LogInformation("Finished selective iCloud calendar wipe.");
	}

	private Dictionary<string, Outlook.AppointmentItem> GetOutlookEvents(Outlook.Items items)
	{
		var events = new Dictionary<string, Outlook.AppointmentItem>();

		items.IncludeRecurrences = true;
		foreach (object item in items)
		{
			if (item is Outlook.AppointmentItem appt)
			{
				// Skip cancelled meetings
				if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
					continue;

				string uid = appt.EntryID ?? Guid.NewGuid().ToString();
				events[uid] = appt;

				if (appt.IsRecurring)
				{
					var pattern = appt.GetRecurrencePattern();
					if (pattern != null)
					{
						foreach (Outlook.Exception ex in pattern.Exceptions)
						{
							var exAppt = ex.AppointmentItem;
							if (exAppt != null && exAppt.MeetingStatus != Outlook.OlMeetingStatus.olMeetingCanceled)
							{
								string exUid = exAppt.EntryID ?? Guid.NewGuid().ToString();
								events[exUid] = exAppt;
							}
						}
						Marshal.ReleaseComObject(pattern);
					}
				}
			}
		}
		return events;
	}

	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, Outlook.AppointmentItem> outlookEvents)
	{
		string calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl);
		_logger.LogInformation("Found {Count} iCloud events for comparison.", iCloudEvents.Count);

		// Process Outlook events (create/update)
		foreach (var (uid, appt) in outlookEvents)
		{
			var calEvent = CreateCalendarEvent(appt, uid);
			var calendar = new Calendar { Events = { calEvent } };
			var serializer = new CalendarSerializer();
			string icsContent = serializer.SerializeToString(calendar);

			string eventUrl = $"{calendarUrl}{uid}.ics";
			var request = new HttpRequestMessage(HttpMethod.Put, eventUrl)
			{
				Content = new StringContent(icsContent, Encoding.UTF8, "text/calendar")
			};

			var response = await client.SendAsync(request);
			if (response.IsSuccessStatusCode)
			{
				_logger.LogInformation("Synced event '{Subject}' with UID {Uid}", appt.Subject, uid);
			}
			else
			{
				_logger.LogWarning("Failed to sync event '{Subject}' with UID {Uid}: {Status} - {Reason}", appt.Subject, uid, response.StatusCode, response.ReasonPhrase);
				await RetryRequestAsync(client, request);
			}
		}

		// Delete iCloud events not in Outlook
		foreach (var iCloudUid in iCloudEvents.Keys)
		{
			if (!outlookEvents.ContainsKey(iCloudUid))
			{
				string eventUrl = $"{calendarUrl}{iCloudUid}.ics";
				var request = new HttpRequestMessage(HttpMethod.Delete, eventUrl);

				var response = await client.SendAsync(request);
				if (response.IsSuccessStatusCode)
				{
					_logger.LogInformation("Deleted iCloud event with UID {Uid}", iCloudUid);
				}
				else
				{
					_logger.LogWarning("Failed to delete iCloud event with UID {Uid}: {Status} - {Reason}", iCloudUid, response.StatusCode, response.ReasonPhrase);
				}
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

		if (appt.IsRecurring)
		{
			var pattern = appt.GetRecurrencePattern();
			if (pattern != null)
			{
				var recurrenceRule = new RecurrencePattern
				{
					Frequency = pattern.RecurrenceType switch
					{
						Outlook.OlRecurrenceType.olRecursDaily => FrequencyType.Daily,
						Outlook.OlRecurrenceType.olRecursWeekly => FrequencyType.Weekly,
						Outlook.OlRecurrenceType.olRecursMonthly => FrequencyType.Monthly,
						Outlook.OlRecurrenceType.olRecursYearly => FrequencyType.Yearly,
						_ => FrequencyType.None
					},
					Interval = pattern.Interval
				};

				if (pattern.RecurrenceType == Outlook.OlRecurrenceType.olRecursWeekly)
				{
					var daysOfWeek = new List<WeekDay>();
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olMonday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Monday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olTuesday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Tuesday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olWednesday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Wednesday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olThursday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Thursday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olFriday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Friday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olSaturday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Saturday));
					if ((pattern.DayOfWeekMask & Outlook.OlDaysOfWeek.olSunday) != 0)
						daysOfWeek.Add(new WeekDay(DayOfWeek.Sunday));
					recurrenceRule.ByDay = daysOfWeek;
				}

				if (pattern.NoEndDate)
					recurrenceRule.Count = null;
				else if (pattern.Occurrences > 0)
					recurrenceRule.Count = pattern.Occurrences;
				else if (pattern.PatternEndDate != DateTime.MinValue)
					recurrenceRule.Until = new CalDateTime(pattern.PatternEndDate.ToUniversalTime());

				calEvent.RecurrenceRules.Add(recurrenceRule);
				Marshal.ReleaseComObject(pattern);
			}
		}

		// Add 10-minute reminder
	
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