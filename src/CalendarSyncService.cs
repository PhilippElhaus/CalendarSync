using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Ical.Net.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml.Linq;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync.src;

public class CalendarSyncService : BackgroundService
{
	private record OutlookEventDto(
	string Subject,
	string Body,
	string Location,
	DateTime Start,
	DateTime End,
	string GlobalId
	);

	private readonly SyncConfig _config;
	private readonly ILogger<CalendarSyncService> _logger;
	private readonly TrayIconManager _tray;
	private static bool _isFirstRun = true;
	private readonly TimeSpan _initialWait;
	private readonly TimeSpan _syncInterval;

	public CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger, TrayIconManager tray)
	{
		_config = config ?? throw new ArgumentNullException(nameof(config));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_tray = tray ?? throw new ArgumentNullException(nameof(tray));
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
				await PerformSyncAsync(stoppingToken);
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

	private async Task PerformSyncAsync(CancellationToken stoppingToken)
	{
		EventLog.WriteEntry("Main", "Started a Sync", EventLogEntryType.Information);
		_tray.SetUpdating();
		_logger.LogInformation("Starting sync at {Time}", DateTime.Now);

		Outlook.Application outlookApp = null;
		Outlook.NameSpace outlookNs = null;
		Outlook.MAPIFolder calendar = null;
		Outlook.Items items = null;

		var retryCount = 0;
		const int maxRetries = 5;
		var connected = false;

		while (retryCount < maxRetries && !connected && !stoppingToken.IsCancellationRequested)
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
				await Task.Delay(10000, stoppingToken);
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
				await Task.Delay(10000, stoppingToken);
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

			var start = DateTime.Today.AddDays(-_config.SyncDaysIntoPast);
			var end = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);

			var filter = $"[Start] <= '{end:g}' AND [End] >= '{start:g}'";
			items = items.Restrict(filter);

			_logger.LogDebug("Applied Outlook Restrict filter: {Filter}", filter);

			var allItems = new List<Outlook.AppointmentItem>();
			var count = 0;

			foreach (var item in items)
			{
				if (count++ > 5000)
				{
					_logger.LogWarning("Aborting calendar item scan after 1000 items to prevent hangs.");
					break;
				}

				try
				{
					if (item is Outlook.AppointmentItem appt)
						allItems.Add(appt);
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
			var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";

			if (_isFirstRun)
			{
				_logger.LogInformation("First run detected, initiating wipe.");
				await WipeICloudCalendarAsync(client, calendarUrl, stoppingToken);
				_isFirstRun = false;
				_tray.SetUpdating();
			}

			await SyncWithICloudAsync(client, outlookEvents, stoppingToken);

			EventLog.WriteEntry("Main", "Finished a Sync", EventLogEntryType.Information);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error during sync processing. Skipping this cycle.");
		}
		finally
		{
			_logger.LogDebug("Cleaning up Outlook COM objects.");
			CleanupOutlook(outlookApp, outlookNs, calendar, items);
			_tray.SetIdle();
		}

		_logger.LogInformation("Sync completed at {Time}", DateTime.Now);
	}

	private async Task WipeICloudCalendarAsync(HttpClient client, string calendarUrl, CancellationToken token)
	{
		_logger.LogInformation("Wiping entire iCloud calendar (past and future events).");
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl);
		_logger.LogInformation("Found {Count} existing iCloud events to delete.", iCloudEvents.Count);

		_tray.SetDeleting();
		var total = iCloudEvents.Count;
		var done = 0;

		foreach (var iCloudUid in iCloudEvents.Keys)
		{
			done++;
			if (total > 0)
				_tray.UpdateText($"Deleting... {done}/{total} ({done * 100 / total}%)");

			var eventUrl = $"{calendarUrl}{iCloudUid}.ics";
			var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
			await Task.Delay(300, token);
			try
			{
				var deleteResponse = await client.SendAsync(deleteRequest);
				if (deleteResponse.IsSuccessStatusCode)
					_logger.LogInformation("Deleted iCloud event with UID {Uid}", iCloudUid);
				else
				{
					_logger.LogWarning("Failed to delete iCloud event UID {Uid}: {Status} - {Reason}", iCloudUid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Exception while deleting iCloud event UID {Uid}", iCloudUid);
				await Task.Delay(5000, token);
			}
		}

		if (total > 0)
			_tray.UpdateText($"Finalzing cleaning run...");

		_logger.LogInformation("Finished full iCloud calendar wipe. Waiting 2 minutes for cache to clear.");
		await Task.Delay(TimeSpan.FromSeconds(30), token);
	}

	private Dictionary<string, OutlookEventDto> GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts)
	{
		var events = new Dictionary<string, OutlookEventDto>();
		var expandedRecurringIds = new HashSet<string>();

		var syncStart = DateTime.Today.AddDays(-_config.SyncDaysIntoPast);
		var syncEnd = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);

		foreach (var appt in appts)
		{
			try
			{
				if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
					continue;

				if (appt.IsRecurring)
				{
					var globalId = appt.GlobalAppointmentID;
					if (expandedRecurringIds.Contains(globalId))
						continue;

					expandedRecurringIds.Add(globalId);

					var instances = ExpandRecurrenceManually(appt, syncStart, syncEnd);
					_logger.LogInformation("Expanded recurring series '{Subject}' to {Count} instances", appt.Subject, instances.Count);

					foreach (var (uid, start, end) in instances)
					{
						var dto = new OutlookEventDto(appt.Subject, appt.Body, appt.Location, start, end, globalId);
						events[uid] = dto;
					}
					continue;
				}

				// Single non-recurring event
				var uid_ = $"outlook-{appt.GlobalAppointmentID}-{appt.Start:yyyyMMddTHHmmss}";
				var dtoItem = new OutlookEventDto(appt.Subject, appt.Body, appt.Location, appt.Start, appt.End, appt.GlobalAppointmentID);
				events[uid_] = dtoItem;
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to process appointment.");
			}
		}
		return events;
	}

	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, OutlookEventDto> outlookEvents, CancellationToken token)
	{
		var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl); // UID -> etag (unused)

		_logger.LogInformation("Found {Count} iCloud events before sync.", iCloudEvents.Count);

		_tray.SetUpdating();
		var total = outlookEvents.Count;
		var done = 0;

		foreach (var (uid, dto) in outlookEvents)
		{
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

			var responsePut = await client.SendAsync(requestPut);
			if (responsePut.IsSuccessStatusCode)
				_logger.LogInformation("Synced event '{Subject}'", dto.Subject);
			else
			{
				_logger.LogWarning("Failed to sync event '{Subject}' UID {Uid}: {Status} - {Reason}",
						dto.Subject, uid, responsePut.StatusCode, responsePut.ReasonPhrase);
				await RetryRequestAsync(client, requestPut, token);
			}
		}

		if (total > 0)
			_tray.UpdateText($"Updating... {total}/{total} (100%)");

		var toDelete = iCloudEvents.Keys.Where(u => !outlookEvents.ContainsKey(u)).ToList();
		var delTotal = toDelete.Count;
		var delDone = 0;

		foreach (var uid in toDelete)
		{
			if (delDone == 0 && delTotal > 0)
				_tray.SetDeleting();

			delDone++;
			if (delTotal > 0)
				_tray.UpdateText($"Deleting... {delDone}/{delTotal} ({delDone * 100 / delTotal}%)");

			var eventUrl = $"{calendarUrl}{uid}.ics";
			var request = new HttpRequestMessage(HttpMethod.Delete, eventUrl);
			var response = await client.SendAsync(request);

			if (response.IsSuccessStatusCode)
				_logger.LogInformation("Deleted orphaned iCloud event UID {Uid}", uid);
			else
				_logger.LogWarning("Failed to delete orphaned iCloud event UID {Uid}: {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
		}

		if (delTotal > 0)
			_tray.UpdateText($"Deleting... {delTotal}/{delTotal} (100%)");
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

	private CalendarEvent CreateCalendarEvent(OutlookEventDto appt, string uid)
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

		// Reminders
		calEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT10M") });
		calEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT3M") });

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

	private async Task RetryRequestAsync(HttpClient client, HttpRequestMessage original, CancellationToken token)
	{
		await Task.Delay(5000, token);
		using var request = new HttpRequestMessage(original.Method, original.RequestUri);

		if (original.Content is StringContent sc)
		{
			var body = await sc.ReadAsStringAsync();
			request.Content = new StringContent(body, Encoding.UTF8, sc.Headers.ContentType?.MediaType ?? "text/plain");
		}

		var retryResponse = await client.SendAsync(request);
		if (!retryResponse.IsSuccessStatusCode)
			_logger.LogError("Retry failed for {Method} {Url}: {Status} - {Reason}", original.Method, original.RequestUri, retryResponse.StatusCode, retryResponse.ReasonPhrase);
	}

	private List<(string uid, DateTime start, DateTime end)> ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to)
	{
		var results = new List<(string uid, DateTime start, DateTime end)>();

		Outlook.RecurrencePattern pattern;
		try
		{
			pattern = appt.GetRecurrencePattern();
		}
		catch (COMException ex)
		{
			_logger.LogDebug(ex, "Failed to get recurrence pattern.");
			return results;
		}

		if (pattern == null)
			return results;

		// Handle unsupported recurrence types
		var freq = pattern.RecurrenceType switch
		{
			Outlook.OlRecurrenceType.olRecursDaily => FrequencyType.Daily,
			Outlook.OlRecurrenceType.olRecursWeekly => FrequencyType.Weekly,
			Outlook.OlRecurrenceType.olRecursMonthly => FrequencyType.Monthly,
			Outlook.OlRecurrenceType.olRecursYearly => FrequencyType.Yearly,
			_ => FrequencyType.None
		};

		if (freq == FrequencyType.None)
		{
			_logger.LogWarning("Unsupported recurrence type for event '{Subject}'. Skipping.", appt.Subject);
			return results;
		}

		// Build recurrence rule
		var rule = new RecurrencePattern
		{
			Frequency = freq,
			Interval = pattern.Interval
		};

		if (freq == FrequencyType.Weekly)
		{
			var byDay = new List<WeekDay>();
			var mask = pattern.DayOfWeekMask;

			if ((mask & Outlook.OlDaysOfWeek.olMonday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Monday));
			if ((mask & Outlook.OlDaysOfWeek.olTuesday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Tuesday));
			if ((mask & Outlook.OlDaysOfWeek.olWednesday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Wednesday));
			if ((mask & Outlook.OlDaysOfWeek.olThursday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Thursday));
			if ((mask & Outlook.OlDaysOfWeek.olFriday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Friday));
			if ((mask & Outlook.OlDaysOfWeek.olSaturday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Saturday));
			if ((mask & Outlook.OlDaysOfWeek.olSunday) != 0)
				byDay.Add(new WeekDay(DayOfWeek.Sunday));

			rule.ByDay = byDay;
		}

		if (!pattern.NoEndDate)
		{
			if (pattern.Occurrences > 0)
				rule.Count = pattern.Occurrences;
			else if (pattern.PatternEndDate != DateTime.MinValue)
				rule.Until = new CalDateTime(pattern.PatternEndDate.ToUniversalTime());
		}

		var calEvent = new CalendarEvent
		{
			Start = new CalDateTime(appt.Start.ToUniversalTime()),
			End = new CalDateTime(appt.End.ToUniversalTime()),
			RecurrenceRules = new List<RecurrencePattern> { rule }
		};

		// Collect exception dates to skip
		var skipDates = new HashSet<DateTime>();
		foreach (Outlook.Exception ex in pattern.Exceptions)
		{
			try
			{
				skipDates.Add(ex.OriginalDate.Date);

				if (ex.AppointmentItem != null)
				{
					var exStart = ex.AppointmentItem.Start;
					var exEnd = ex.AppointmentItem.End;

					if (exStart >= from && exStart <= to)
					{
						var exUid = $"outlook-{appt.GlobalAppointmentID}-{exStart:yyyyMMddTHHmmss}";
						results.Add((exUid, exStart, exEnd));
						_logger.LogInformation("Processed modified occurrence for '{Subject}' at {Start}", appt.Subject, exStart);
					}
				}
			}
			catch { /* skip broken exceptions */ }
		}

		// Evaluate occurrences
		var occurrences = calEvent.GetOccurrences(from.ToUniversalTime(), to.ToUniversalTime());

		foreach (var occ in occurrences)
		{
			var start = occ.Period.StartTime.Value.ToLocalTime();
			if (skipDates.Contains(start.Date))
				continue;

			var end = start.Add(appt.End - appt.Start);
			var uid = $"outlook-{appt.GlobalAppointmentID}-{start:yyyyMMddTHHmmss}";
			results.Add((uid, start, end));
		}

		return results;
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