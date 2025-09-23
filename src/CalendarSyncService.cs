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
using System.Windows.Forms;
using Outlook = Microsoft.Office.Interop.Outlook;
using System.Net;

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
	private readonly string _sourceId;
	private readonly string? _tag;
	private readonly SemaphoreSlim _opLock = new SemaphoreSlim(1, 1);
	private CancellationTokenSource _currentOpCts = new CancellationTokenSource();

	public CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger, TrayIconManager tray)
	{
		_config = config ?? throw new ArgumentNullException(nameof(config));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_tray = tray ?? throw new ArgumentNullException(nameof(tray));
		_initialWait = TimeSpan.FromSeconds(_config.InitialWaitSeconds);
		_syncInterval = TimeSpan.FromMinutes(_config.SyncIntervalMinutes);
		_sourceId = _config.SourceId ?? "";
		_tag = string.IsNullOrWhiteSpace(_config.EventTag) ? null : _config.EventTag!.Trim();
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_logger.LogInformation("Calendar Sync Service started.");
		EventRecorder.WriteEntry("Service started", EventLogEntryType.Information);

		_logger.LogInformation("Initial wait for {InitialWait} seconds before starting sync.", _initialWait.TotalSeconds);
		await Task.Delay(_initialWait, stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
		{
			_currentOpCts = new CancellationTokenSource();
			await _opLock.WaitAsync(stoppingToken);
			var token = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _currentOpCts.Token).Token;
			try
			{
				await PerformSyncAsync(token);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Unexpected error during sync. Continuing to next cycle.");
			}
			finally
			{
				_opLock.Release();
			}
			_logger.LogDebug("Waiting for next sync cycle.");
			await Task.Delay(_syncInterval, stoppingToken);
		}
		_logger.LogInformation("Calendar Sync Service stopped.");
		EventRecorder.WriteEntry("Service stopped", EventLogEntryType.Information);
	}

	private async Task PerformSyncAsync(CancellationToken stoppingToken)
	{
		EventRecorder.WriteEntry("Sync started", EventLogEntryType.Information);
		_tray.SetUpdating();
		_logger.LogInformation("Starting sync at {Time}", DateTime.Now);

		try
		{
		var outlookEvents = await FetchOutlookEventsAsync(stoppingToken);

		using var client = CreateHttpClient();
		var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";

		if (_isFirstRun)
		{
				_logger.LogInformation("First run detected, initiating wipe.");
				await WipeICloudCalendarAsync(client, calendarUrl, stoppingToken, true);
				_isFirstRun = false;
				_tray.SetUpdating();
		}

		await SyncWithICloudAsync(client, outlookEvents, stoppingToken);

			EventRecorder.WriteEntry("Sync finished", EventLogEntryType.Information);
}
		catch (UnauthorizedAccessException ex)
{
			_logger.LogError(ex, "iCloud authorization failed. Check credentials.");
			EventRecorder.WriteEntry("iCloud authorization failed", EventLogEntryType.Error);
			MessageBox.Show("iCloud authorization failed. Check credentials.", "CalendarSync", MessageBoxButtons.OK, MessageBoxIcon.Error);
}
		catch (OperationCanceledException)
{
			_logger.LogError("Outlook operation timed out.");
			EventRecorder.WriteEntry("Outlook operation timed out", EventLogEntryType.Error);
}
		catch (Exception ex)
{
			_logger.LogError(ex, "Error during sync processing. Skipping this cycle.");
}
		finally
		{
		_tray.SetIdle();
		}

		_logger.LogInformation("Sync completed at {Time}", DateTime.Now);
	}

	private Task<Dictionary<string, OutlookEventDto>> FetchOutlookEventsAsync(CancellationToken token)
	{
		var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
		cts.CancelAfter(TimeSpan.FromMinutes(2));

		return StaTask.Run(() =>
		{
		Outlook.Application outlookApp = null;
		Outlook.NameSpace outlookNs = null;
		Outlook.MAPIFolder calendar = null;
		Outlook.Items items = null;

		try
		{
				var retryCount = 0;
				const int maxRetries = 5;

				while (retryCount < maxRetries && !cts.Token.IsCancellationRequested)
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
				break;
				}
				catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
				{
				retryCount++;
				_logger.LogWarning(ex, $"Failed to connect to Outlook (CO_E_SERVER_EXEC_FAILURE), retry {retryCount}/{maxRetries}.");
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
				if (retryCount == maxRetries)
							throw;
				_logger.LogDebug("Waiting 10 seconds before retry.");
				Task.Delay(10000, cts.Token).Wait(cts.Token);
				}
				catch (Exception ex)
				{
				retryCount++;
				_logger.LogWarning(ex, "Unexpected error connecting to Outlook, retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
				if (retryCount == maxRetries)
							throw;
				_logger.LogDebug("Waiting 10 seconds before retry.");
				Task.Delay(10000, cts.Token).Wait(cts.Token);
				}
				}

				if (items == null)
				{
				_logger.LogDebug("No connection established, exiting FetchOutlookEventsAsync.");
				return new Dictionary<string, OutlookEventDto>();
				}

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

				return outlookEvents;
		}
		finally
		{
				_logger.LogDebug("Cleaning up Outlook COM objects.");
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
		}
		}, cts.Token);
	}

	private async Task WipeICloudCalendarAsync(HttpClient client, string calendarUrl, CancellationToken token, bool filterBySource)
	{
		if (filterBySource)
			_logger.LogInformation("Cleaning existing events for source {SourceId}.", _sourceId);
		else
			_logger.LogInformation("Cleaning all existing iCloud events.");

		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, filterBySource);
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

					if (deleteResponse.StatusCode == HttpStatusCode.Unauthorized || deleteResponse.StatusCode == HttpStatusCode.Forbidden)
						throw new UnauthorizedAccessException("iCloud authentication failed.");
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

	public async Task WipeEntireCalendarAsync()
	{
		_currentOpCts.Cancel();
		await _opLock.WaitAsync();
		try
		{
			_currentOpCts = new CancellationTokenSource();
			var token = _currentOpCts.Token;
			using var client = CreateHttpClient();
			var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
			await WipeICloudCalendarAsync(client, calendarUrl, token, false);
		}
		catch (UnauthorizedAccessException ex)
		{
			_logger.LogError(ex, "iCloud authorization failed. Check credentials.");
			EventRecorder.WriteEntry("iCloud authorization failed", EventLogEntryType.Error);
			MessageBox.Show("iCloud authorization failed. Check credentials.", "CalendarSync", MessageBoxButtons.OK, MessageBoxIcon.Error);
		}
		finally
		{
			_opLock.Release();
		}
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
				AddEventChunks(events, uid, dto);
				}
				continue;
				}

				// Single non-recurring event
				var uid_ = $"outlook-{appt.GlobalAppointmentID}-{appt.Start:yyyyMMddTHHmmss}";
				var dtoItem = new OutlookEventDto(appt.Subject, appt.Body, appt.Location, appt.Start, appt.End, appt.GlobalAppointmentID);
				AddEventChunks(events, uid_, dtoItem);
		}
		catch (Exception ex)
		{
				_logger.LogWarning(ex, "Failed to process appointment.");
		}
		finally
		{
				try
				{
				Marshal.FinalReleaseComObject(appt);
				}
				catch { }
		}
		}

		return events;
	}

	private void AddEventChunks(Dictionary<string, OutlookEventDto> events, string baseUid, OutlookEventDto dto)
	{
		var span = dto.End - dto.Start;
		var isAllDay = dto.Start.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
		(dto.End.TimeOfDay == TimeSpan.Zero || dto.End.TimeOfDay >= new TimeSpan(23, 59, 0));

		if (isAllDay)
		{
		var endDate = dto.End.TimeOfDay == TimeSpan.Zero ? dto.End.Date : dto.End.Date.AddDays(1);
		var days = (endDate - dto.Start.Date).Days;

		if (days > 1)
		{
				for (var i = 0; i < days; i++)
				{
				var dayStart = dto.Start.Date.AddDays(i);
				var dayEnd = dayStart.AddDays(1);
				var uid = $"{_sourceId}-{baseUid}-{dayStart:yyyyMMdd}";
				var dayDto = new OutlookEventDto(dto.Subject, dto.Body, dto.Location, dayStart, dayEnd, dto.GlobalId);
				events[uid] = dayDto;
				}
				return;
		}
		}

		events[$"{_sourceId}-{baseUid}"] = dto;
	}

	private async Task SyncWithICloudAsync(HttpClient client, Dictionary<string, OutlookEventDto> outlookEvents, CancellationToken token)
	{
		var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
		var iCloudEvents = await GetICloudEventsAsync(client, calendarUrl, true); // UID -> etag (unused)

		_logger.LogInformation("Found {Count} iCloud events before sync.", iCloudEvents.Count);

		var desiredUids = new HashSet<string>(outlookEvents.Keys, StringComparer.OrdinalIgnoreCase);
		var staleUids = iCloudEvents.Keys.Where(uid => IsManagedUid(uid) && !desiredUids.Contains(uid)).ToList();

		if (staleUids.Count > 0)
		{
			_logger.LogInformation("Deleting {Count} stale iCloud events before applying updates.", staleUids.Count);
			_tray.SetDeleting();
			var delTotal = staleUids.Count;
			var delDone = 0;

			foreach (var uid in staleUids)
			{
				token.ThrowIfCancellationRequested();

				delDone++;
				_tray.UpdateText($"Deleting... {delDone}/{delTotal} ({delDone * 100 / delTotal}%)");

				var deleteUrl = $"{calendarUrl}{uid}.ics";
				var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, deleteUrl);
				var deleteResponse = await client.SendAsync(deleteRequest, token);

				if (deleteResponse.IsSuccessStatusCode)
				{
					_logger.LogInformation("Deleted stale iCloud event UID {Uid}", uid);
				}
				else
				{
					if (deleteResponse.StatusCode == HttpStatusCode.Unauthorized || deleteResponse.StatusCode == HttpStatusCode.Forbidden)
						throw new UnauthorizedAccessException("iCloud authentication failed.");
					_logger.LogWarning("Failed to delete stale iCloud event UID {Uid}: {Status} - {Reason}", uid, deleteResponse.StatusCode, deleteResponse.ReasonPhrase);
					await RetryRequestAsync(client, deleteRequest, token);
				}
			}

			_tray.SetUpdating();
		}
		else
		{
			_logger.LogInformation("No stale iCloud events detected prior to sync.");
			_tray.SetUpdating();
		}

		var total = outlookEvents.Count;
		var done = 0;

		foreach (var (uid, dto) in outlookEvents)
		{
			token.ThrowIfCancellationRequested();
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

			var responsePut = await client.SendAsync(requestPut, token);
			if (responsePut.IsSuccessStatusCode)
				_logger.LogInformation("Synced event '{Subject}'", dto.Subject);
			else
			{
				if (responsePut.StatusCode == HttpStatusCode.Unauthorized || responsePut.StatusCode == HttpStatusCode.Forbidden)
					throw new UnauthorizedAccessException("iCloud authentication failed.");
				_logger.LogWarning("Failed to sync event '{Subject}' UID {Uid}: {Status} - {Reason}",
								dto.Subject, uid, responsePut.StatusCode, responsePut.ReasonPhrase);
				await RetryRequestAsync(client, requestPut, token);
			}
		}

		if (total > 0)
			_tray.UpdateText($"Updating... {total}/{total} (100%)");
	}

	private async Task<Dictionary<string, string>> GetICloudEventsAsync(HttpClient client, string calendarUrl, bool filterBySource)
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
			EventRecorder.WriteEntry("iCloud fetch failed", EventLogEntryType.Error);
			if (response.StatusCode == HttpStatusCode.Unauthorized || response.StatusCode == HttpStatusCode.Forbidden)
				throw new UnauthorizedAccessException("iCloud authentication failed.");
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
				if (filterBySource && !IsManagedUid(uid))
				{
					_logger.LogDebug("Skipping non-managed iCloud event UID: {Uid}", uid);
					continue;
				}
				events[uid] = "";
				_logger.LogDebug("Found iCloud event UID: {Uid}", uid);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to parse PROPFIND response: {Content}", content);
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

		// Convert 24h+ spans starting at midnight to all-day events
		var span = appt.End - appt.Start;
		var isAllDay = appt.Start.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
		(appt.End.TimeOfDay == TimeSpan.Zero || appt.End.TimeOfDay >= new TimeSpan(23, 59, 0));

		if (isAllDay)
		{
		start = new CalDateTime(appt.Start.Date, tzId: null, hasTime: false);
		var endDate = appt.End.TimeOfDay == TimeSpan.Zero ? appt.End.Date : appt.End.Date.AddDays(1);
		end = new CalDateTime(endDate, tzId: null, hasTime: false);
		}
		else
		{
		start = new CalDateTime(appt.Start.ToUniversalTime());
		end = new CalDateTime(appt.End.ToUniversalTime());
		}

		var calEvent = new CalendarEvent
		{
		Summary = summary,
		Start = start,
		End = end,
		Location = appt.Location ?? "",
		Uid = uid,
		Description = appt.Body ?? ""
		};

		// Reminders
		if (!isAllDay)
		{
		calEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT10M") });
		calEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT3M") });
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
		var prefixes = string.IsNullOrEmpty(_sourceId)
			? new[] { "-outlook-" }
			: new[] { $"{_sourceId}-outlook-", "-outlook-" };

		foreach (var prefix in prefixes)
		{
			if (normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
				return true;
		}

		return false;
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

		var retryResponse = await client.SendAsync(request, token);
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

		var apptIsMaster = false;
		try
		{
			apptIsMaster = appt.RecurrenceState == Outlook.OlRecurrenceState.olApptMaster;
		}
		catch (COMException)
		{
		}

		DateTime? masterStart = null;
		DateTime? masterEnd = null;
		Outlook.AppointmentItem? master = null;
		var releaseMaster = false;

		if (!apptIsMaster)
		{
			try
			{
				master = pattern.Parent as Outlook.AppointmentItem;
				if (master != null && !ReferenceEquals(master, appt))
				{
					releaseMaster = true;
					try
					{
						masterStart = master.Start;
						masterEnd = master.End;
					}
					catch (COMException)
					{
						masterStart = null;
						masterEnd = null;
					}
				}
				else if (master != null)
				{
					masterStart = appt.Start;
					masterEnd = appt.End;
				}
			}
			catch (COMException)
			{
			}
			finally
			{
				if (releaseMaster && master != null)
				{
					try
					{
						Marshal.ReleaseComObject(master);
					}
					catch
					{
					}
				}
			}
		}

		var seriesStart = pattern.StartTime != DateTime.MinValue
			? pattern.StartTime
			: masterStart ?? appt.Start;

		var baseDuration = TimeSpan.Zero;
		if (pattern.StartTime != DateTime.MinValue && pattern.EndTime != DateTime.MinValue)
		{
			var candidate = pattern.EndTime - pattern.StartTime;
			if (candidate > TimeSpan.Zero)
				baseDuration = candidate;
		}

		if (baseDuration <= TimeSpan.Zero && masterStart.HasValue && masterEnd.HasValue)
		{
			var candidate = masterEnd.Value - masterStart.Value;
			if (candidate > TimeSpan.Zero)
			{
				if (pattern.StartTime == DateTime.MinValue)
					seriesStart = masterStart.Value;
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero && apptIsMaster)
		{
			var candidate = appt.End - appt.Start;
			if (candidate > TimeSpan.Zero)
			{
				seriesStart = appt.Start;
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero)
		{
			var candidate = appt.End - appt.Start;
			if (candidate > TimeSpan.Zero)
				baseDuration = candidate;
		}

		if (seriesStart == DateTime.MinValue)
			seriesStart = appt.Start;

		var seriesEnd = seriesStart.Add(baseDuration);
		var calEvent = new CalendarEvent
		{
			Start = new CalDateTime(seriesStart.ToUniversalTime()),
			End = new CalDateTime(seriesEnd.ToUniversalTime()),
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

			var end = start.Add(baseDuration);
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
				Marshal.FinalReleaseComObject(items);
		if (folder != null)
				Marshal.FinalReleaseComObject(folder);
		if (ns != null)
				Marshal.FinalReleaseComObject(ns);
		if (app != null)
				Marshal.FinalReleaseComObject(app);
		}
		catch
		{
		_logger.LogError("Unable to clean up Outlook COM objects.");
		}
	}
}
