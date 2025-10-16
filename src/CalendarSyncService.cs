using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Ical.Net.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml.Linq;
using Outlook = Microsoft.Office.Interop.Outlook;
using System.Linq;

namespace CalendarSync.src;

public class CalendarSyncService : BackgroundService
{
	private record OutlookEventDto(
	string Subject,
	string Body,
	string Location,
	DateTime StartLocal,
	DateTime EndLocal,
	DateTime StartUtc,
	DateTime EndUtc,
	string GlobalId
	);

	private readonly SyncConfig _config;
	private readonly ILogger<CalendarSyncService> _logger;
	private readonly TrayIconManager _tray;
	private static bool _isFirstRun = true;
	private const double TimezoneSanityToleranceMinutes = 1;
	private readonly TimeSpan _initialWait;
	private readonly TimeSpan _syncInterval;
	private readonly string _sourceId;
	private readonly string? _tag;
	private readonly TimeZoneInfo _sourceTimeZone;
	private readonly TimeZoneInfo _targetTimeZone;
	private readonly SemaphoreSlim _opLock = new SemaphoreSlim(1, 1);
	private CancellationTokenSource _currentOpCts = new CancellationTokenSource();
	private CancellationToken _serviceStoppingToken = CancellationToken.None;
	private static readonly Guid OutlookApplicationClsid = new("0006F03A-0000-0000-C000-000000000046");

	[DllImport("oleaut32.dll")]
	private static extern int GetActiveObjectNative(ref Guid rclsid, IntPtr pvReserved, [MarshalAs(UnmanagedType.IUnknown)] out object? ppunk);

	public CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger, TrayIconManager tray)
	{
		_config = config ?? throw new ArgumentNullException(nameof(config));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_tray = tray ?? throw new ArgumentNullException(nameof(tray));
		_initialWait = TimeSpan.FromSeconds(_config.InitialWaitSeconds);
		_syncInterval = TimeSpan.FromMinutes(_config.SyncIntervalMinutes);
		_sourceId = _config.SourceId ?? "";
		_tag = string.IsNullOrWhiteSpace(_config.EventTag) ? null : _config.EventTag!.Trim();
		_sourceTimeZone = ResolveTimeZone(_config.SourceTimeZoneId, "source");
		_targetTimeZone = ResolveTimeZone(_config.TargetTimeZoneId, "target");
		if (!_sourceTimeZone.Id.Equals(_targetTimeZone.Id, StringComparison.OrdinalIgnoreCase))
			_logger.LogInformation("Source timezone {Source} and target timezone {Target} differ; synchronizing via UTC conversions.", _sourceTimeZone.Id, _targetTimeZone.Id);
		}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_serviceStoppingToken = stoppingToken;

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
		catch (OperationCanceledException ex)
		{
			if (_serviceStoppingToken.IsCancellationRequested)
			{
				_logger.LogInformation("Sync canceled because the service is stopping.");
			}
			else if (_currentOpCts.IsCancellationRequested)
			{
				_logger.LogInformation("Sync canceled in preparation for a manual full re-sync.");
			}
			else
			{
				_logger.LogError(ex, "Outlook operation timed out.");
				EventRecorder.WriteEntry("Outlook operation timed out", EventLogEntryType.Error);
			}
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
						cts.Token.ThrowIfCancellationRequested();
						_logger.LogDebug("Attempting to create Outlook.Application instance.");
						outlookApp = CreateOutlookApplication(cts.Token);
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
						outlookApp = null;
						outlookNs = null;
						calendar = null;
						items = null;
						if (retryCount == maxRetries)
							throw;
						EnsureOutlookProcessReady(cts.Token);
						_logger.LogDebug("Waiting 10 seconds before retry.");
						DelayWithCancellation(TimeSpan.FromSeconds(10), cts.Token);
					}
					catch (OperationCanceledException)
					{
						CleanupOutlook(outlookApp, outlookNs, calendar, items);
						throw;
					}
					catch (Exception ex)
					{
						retryCount++;
						_logger.LogWarning(ex, "Unexpected error connecting to Outlook, retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
						CleanupOutlook(outlookApp, outlookNs, calendar, items);
						outlookApp = null;
						outlookNs = null;
						calendar = null;
						items = null;
						if (retryCount == maxRetries)
							throw;
						EnsureOutlookProcessReady(cts.Token);
						_logger.LogDebug("Waiting 10 seconds before retry.");
						DelayWithCancellation(TimeSpan.FromSeconds(10), cts.Token);
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

	public async Task TriggerFullResyncAsync()
	{
		EventRecorder.WriteEntry("Manual full re-sync requested", EventLogEntryType.Information);
		_currentOpCts.Cancel();
		await _opLock.WaitAsync();
		try
		{
			_currentOpCts = new CancellationTokenSource();
			using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_currentOpCts.Token, _serviceStoppingToken);
			var token = linkedCts.Token;
			using var client = CreateHttpClient();
			var calendarUrl = $"{_config.ICloudCalDavUrl}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";
			await WipeICloudCalendarAsync(client, calendarUrl, token, false);
			token.ThrowIfCancellationRequested();
			_tray.SetUpdating();
			await PerformSyncAsync(token);
		}
		catch (UnauthorizedAccessException ex)
		{
			_logger.LogError(ex, "iCloud authorization failed. Check credentials.");
			EventRecorder.WriteEntry("iCloud authorization failed", EventLogEntryType.Error);
			MessageBox.Show("iCloud authorization failed. Check credentials.", "CalendarSync", MessageBoxButtons.OK, MessageBoxIcon.Error);
		}
		catch (OperationCanceledException)
		{
			_logger.LogInformation("Manual full re-sync canceled.");
		}
		finally
		{
			_opLock.Release();
		}
	}

	private Dictionary<string, OutlookEventDto> GetOutlookEventsFromList(List<Outlook.AppointmentItem> appts)
	{
		var events = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var expandedRecurringIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		var sourceToday = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, _sourceTimeZone).Date;
		var syncStart = sourceToday.AddDays(-_config.SyncDaysIntoPast);
		var syncEnd = sourceToday.AddDays(_config.SyncDaysIntoFuture);

		foreach (var appt in appts)
		{
			try
			{
				if (appt.MeetingStatus == Outlook.OlMeetingStatus.olMeetingCanceled)
					continue;

				if (appt.IsRecurring)
				{
					Outlook.AppointmentItem seriesItem = appt;
					Outlook.AppointmentItem? masterItem = null;
					var shouldReleaseMaster = false;
					var globalId = appt.GlobalAppointmentID;

					var recurrenceState = Outlook.OlRecurrenceState.olApptMaster;
					try
					{
						recurrenceState = appt.RecurrenceState;
					}
					catch (COMException ex)
					{
						_logger.LogDebug(ex, "Failed to read recurrence state for '{Subject}'. Assuming master.", appt.Subject);
					}

					if (recurrenceState != Outlook.OlRecurrenceState.olApptMaster)
					{
						try
						{
							var pattern = appt.GetRecurrencePattern();
							if (pattern?.Parent is Outlook.AppointmentItem parent)
							{
								masterItem = parent;
								if (!ReferenceEquals(parent, appt))
								{
									shouldReleaseMaster = true;
									seriesItem = parent;
								}
								try
								{
									if (!string.IsNullOrEmpty(parent.GlobalAppointmentID))
										globalId = parent.GlobalAppointmentID;
								}
								catch (COMException)
								{
								}
							}
						}
						catch (COMException ex)
						{
							_logger.LogDebug(ex, "Failed to resolve master item for '{Subject}'.", appt.Subject);
						}
					}

					if (string.IsNullOrEmpty(globalId))
						globalId = appt.GlobalAppointmentID;

					if (string.IsNullOrEmpty(globalId))
						globalId = Guid.NewGuid().ToString();

					    if (!expandedRecurringIds.Add(globalId))
                                        {
                                                if (shouldReleaseMaster && masterItem != null)
                                                {
                                                        try
                                                        {
                                                                Marshal.FinalReleaseComObject(masterItem);
                                                        }
                                                        catch { }
                                                }
                                                continue;
                                        }

					try
					{
						var instances = ExpandRecurrenceManually(seriesItem, syncStart, syncEnd);
						_logger.LogInformation("Expanded recurring series '{Subject}' to {Count} instances", seriesItem.Subject, instances.Count);

						foreach (var (uid, startLocal, endLocal, startUtc, endUtc) in instances)
						{
							var dto = new OutlookEventDto(seriesItem.Subject, seriesItem.Body, seriesItem.Location, startLocal, endLocal, startUtc, endUtc, globalId);
							AddEventChunks(events, uid, dto);
						}
					}
					finally
					{
						if (shouldReleaseMaster && masterItem != null)
						{
							try
							{
								Marshal.FinalReleaseComObject(masterItem);
							}
							catch { }
						}
					}
					continue;
				}

				// Single non-recurring event
				var uid_ = $"outlook-{appt.GlobalAppointmentID}-{appt.Start:yyyyMMddTHHmmss}";
				var (singleStartLocal, singleStartUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"event '{appt.Subject}' start");
				var (singleEndLocal, singleEndUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"event '{appt.Subject}' end");
				var dtoItem = new OutlookEventDto(appt.Subject, appt.Body, appt.Location, singleStartLocal, singleEndLocal, singleStartUtc, singleEndUtc, appt.GlobalAppointmentID);
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

		return DeduplicateEvents(events);
	}

	private void AddEventChunks(Dictionary<string, OutlookEventDto> events, string baseUid, OutlookEventDto dto)
	{
		var sanitizedDto = EnsureEventConsistency(dto, baseUid);
		var span = sanitizedDto.EndLocal - sanitizedDto.StartLocal;
		var isAllDay = sanitizedDto.StartLocal.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
			(sanitizedDto.EndLocal.TimeOfDay == TimeSpan.Zero || sanitizedDto.EndLocal.TimeOfDay >= new TimeSpan(23, 59, 0));

		if (isAllDay)
		{
			var endDate = sanitizedDto.EndLocal.TimeOfDay == TimeSpan.Zero ? sanitizedDto.EndLocal.Date : sanitizedDto.EndLocal.Date.AddDays(1);
			var days = (endDate - sanitizedDto.StartLocal.Date).Days;

			if (days > 1)
			{
				for (var i = 0; i < days; i++)
				{
					var dayStartLocal = sanitizedDto.StartLocal.Date.AddDays(i);
					var dayEndLocal = dayStartLocal.AddDays(1);
					var dayStartUtc = ConvertFromSourceLocalToUtc(dayStartLocal, $"{baseUid} day {i + 1} start");
					var dayEndUtc = ConvertFromSourceLocalToUtc(dayEndLocal, $"{baseUid} day {i + 1} end");
					var uid = $"{_sourceId}-{baseUid}-{dayStartLocal:yyyyMMdd}";
					var dayDto = new OutlookEventDto(sanitizedDto.Subject, sanitizedDto.Body, sanitizedDto.Location, dayStartLocal, dayEndLocal, dayStartUtc, dayEndUtc, sanitizedDto.GlobalId);
					var sanitizedDay = EnsureEventConsistency(dayDto, uid);
					events[uid] = sanitizedDay;
				}
				return;
			}
		}

		events[$"{_sourceId}-{baseUid}"] = sanitizedDto;
	}
	private Dictionary<string, OutlookEventDto> DeduplicateEvents(Dictionary<string, OutlookEventDto> events)
	{
		var deduped = new Dictionary<string, OutlookEventDto>(StringComparer.OrdinalIgnoreCase);
		var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		foreach (var (uid, dto) in events)
		{
			if (dto == null)
				continue;

			var globalId = dto.GlobalId ?? string.Empty;
			var signature = $"{globalId}|{dto.StartUtc:O}|{dto.EndUtc:O}";

			if (!seenKeys.Add(signature))
			{
				_logger.LogWarning("Detected duplicate Outlook event for GlobalID {GlobalId} at {Start}. Dropping UID {Uid}.", globalId, dto.StartLocal, uid);
				continue;
			}

			deduped[uid] = dto;
		}

		return deduped;
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
			{
				_logger.LogInformation("Synced event '{Subject}'", dto.Subject);
				var verified = await VerifyICloudEventAsync(client, eventUrl, dto, token);
				if (!verified)
				{
					await AttemptICloudCorrectionAsync(client, eventUrl, newIcs, dto, token);
				}
			}
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

	private async Task<bool> VerifyICloudEventAsync(HttpClient client, string eventUrl, OutlookEventDto dto, CancellationToken token)
	{
		var uid = ExtractUidFromUrl(eventUrl);
		try
		{
			var response = await client.GetAsync(eventUrl, token);
			if (!response.IsSuccessStatusCode)
			{
				_logger.LogWarning("Verification skipped for UID {Uid}: GET returned {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
				return false;
			}

			var ics = await response.Content.ReadAsStringAsync();
			var calendar = Calendar.Load(ics);
			var calEvent = calendar?.Events?.FirstOrDefault();
			if (calEvent == null)
			{
				LogVerificationFailure(uid, "iCloud response contained no events");
				return false;
			}

			var expected = GetExpectedTimes(dto);
			var actual = GetActualTimes(calEvent);
			var tolerance = TimeSpan.FromMinutes(2);

			if (expected.isAllDay != actual.isAllDay)
			{
				LogVerificationFailure(uid, $"expected all-day {expected.isAllDay} but found {actual.isAllDay}");
				return false;
			}

			var matches = expected.isAllDay
				? expected.start.Date == actual.start.Date && expected.end.Date == actual.end.Date
				: IsWithinTolerance(actual.start, expected.start, tolerance) && IsWithinTolerance(actual.end, expected.end, tolerance);

			if (!matches)
			{
				LogVerificationFailure(uid, $"expected {expected.start:o}-{expected.end:o} but found {actual.start:o}-{actual.end:o}");
				return false;
			}

			_logger.LogInformation("Verification confirmed UID {Uid} matches source timings", uid);
			return true;
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Verification failed for UID {Uid}", uid);
			EventRecorder.WriteEntry($"iCloud verification failed UID {uid}", EventLogEntryType.Error);
			return false;
		}
	}

	private async Task AttemptICloudCorrectionAsync(HttpClient client, string eventUrl, string newIcs, OutlookEventDto dto, CancellationToken token)
	{
		var uid = ExtractUidFromUrl(eventUrl);
		try
		{
			_logger.LogWarning("Attempting to correct iCloud event UID {Uid} after verification mismatch", uid);
			using var request = new HttpRequestMessage(HttpMethod.Put, eventUrl);
			request.Content = new StringContent(newIcs, Encoding.UTF8, "text/calendar");
			var response = await client.SendAsync(request, token);
			if (!response.IsSuccessStatusCode)
			{
				if (response.StatusCode == HttpStatusCode.Unauthorized || response.StatusCode == HttpStatusCode.Forbidden)
					throw new UnauthorizedAccessException("iCloud authentication failed.");
				_logger.LogError("Correction PUT failed for UID {Uid}: {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
				EventRecorder.WriteEntry($"iCloud correction failed UID {uid}", EventLogEntryType.Error);
				return;
			}

			var verified = await VerifyICloudEventAsync(client, eventUrl, dto, token);
			if (verified)
			{
				_logger.LogInformation("Verification succeeded after correction for UID {Uid}", uid);
			}
			else
			{
				_logger.LogError("Verification still failing after correction for UID {Uid}", uid);
				EventRecorder.WriteEntry($"iCloud verification still mismatched UID {uid}", EventLogEntryType.Error);
			}
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to correct iCloud event UID {Uid}", uid);
			EventRecorder.WriteEntry($"iCloud correction exception UID {uid}", EventLogEntryType.Error);
		}
	}


	private void LogVerificationFailure(string uid, string message)
	{
		_logger.LogError("Verification mismatch for UID {Uid}: {Message}", uid, message);
		EventRecorder.WriteEntry($"iCloud verification mismatch UID {uid}: {message}", EventLogEntryType.Error);
	}

	private (DateTime start, DateTime end, bool isAllDay) GetExpectedTimes(OutlookEventDto dto)
	{
		var span = dto.EndLocal - dto.StartLocal;
		var isAllDay = dto.StartLocal.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
			(dto.EndLocal.TimeOfDay == TimeSpan.Zero || dto.EndLocal.TimeOfDay >= new TimeSpan(23, 59, 0));

		if (isAllDay)
		{
			var endDate = dto.EndLocal.TimeOfDay == TimeSpan.Zero ? dto.EndLocal.Date : dto.EndLocal.Date.AddDays(1);
			return (dto.StartLocal.Date, endDate, true);
		}

		return (dto.StartUtc, dto.EndUtc, false);
	}


	private static (DateTime start, DateTime end, bool isAllDay) GetActualTimes(CalendarEvent calEvent)
	{
		if (calEvent.IsAllDay || !calEvent.Start.HasTime)
		{
			var startDate = calEvent.Start.Value.Date;
			var endDate = calEvent.End.Value.Date;
			return (startDate, endDate, true);
		}

		return (calEvent.Start.AsUtc, calEvent.End.AsUtc, false);
	}

	private static bool IsWithinTolerance(DateTime actual, DateTime expected, TimeSpan tolerance)
	{
		return Math.Abs((actual - expected).TotalSeconds) <= tolerance.TotalSeconds;
	}

	private static string ExtractUidFromUrl(string eventUrl)
	{
		if (string.IsNullOrEmpty(eventUrl))
			return string.Empty;

		var trimmed = eventUrl.TrimEnd('/');
		var segment = trimmed.Split('/', StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? string.Empty;
		return segment.EndsWith(".ics", StringComparison.OrdinalIgnoreCase) ? segment[..^4] : segment;
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
		var span = appt.EndLocal - appt.StartLocal;
		var isAllDay = appt.StartLocal.TimeOfDay == TimeSpan.Zero && span.TotalHours >= 23 &&
			(appt.EndLocal.TimeOfDay == TimeSpan.Zero || appt.EndLocal.TimeOfDay >= new TimeSpan(23, 59, 0));

                if (isAllDay)
                {
                        var startDate = appt.StartLocal.Date;
                        var endDate = appt.EndLocal.TimeOfDay == TimeSpan.Zero ? appt.EndLocal.Date : appt.EndLocal.Date.AddDays(1);
                        start = new CalDateTime(startDate) { HasTime = false, IsFloating = true };
                        end = new CalDateTime(endDate) { HasTime = false, IsFloating = true };
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
                        Location = appt.Location ?? "",
                        Uid = uid,
                        Description = appt.Body ?? "",
                        IsAllDay = isAllDay
                };

                if (isAllDay)
                {
                        calEvent.Start.HasTime = false;
                        calEvent.End.HasTime = false;
                }

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

	private List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)> ExpandRecurrenceManually(Outlook.AppointmentItem appt, DateTime from, DateTime to)
{
		var results = new List<(string uid, DateTime startLocal, DateTime endLocal, DateTime startUtc, DateTime endUtc)>();

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

		var (baseStartLocal, baseStartUtc) = NormalizeOutlookTimes(appt.Start, appt.StartUTC, $"series '{appt.Subject}' start");
		var (baseEndLocal, baseEndUtc) = NormalizeOutlookTimes(appt.End, appt.EndUTC, $"series '{appt.Subject}' end");

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
					var (resolvedMasterStart, _) = NormalizeOutlookTimes(master.Start, master.StartUTC, $"master '{appt.Subject}' start");
					var (resolvedMasterEnd, _) = NormalizeOutlookTimes(master.End, master.EndUTC, $"master '{appt.Subject}' end");
					masterStart = resolvedMasterStart;
					masterEnd = resolvedMasterEnd;
}
					catch (COMException)
					{
						masterStart = null;
						masterEnd = null;
					}
				}
else if (master != null)
{
					masterStart = baseStartLocal;
					masterEnd = baseEndLocal;
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

		DateTime? patternSeriesStart = null;
		try
		{
			if (pattern.PatternStartDate != DateTime.MinValue)
			{
			var startDate = pattern.PatternStartDate.Date;
			var timeOfDay = pattern.StartTime != DateTime.MinValue
? pattern.StartTime.TimeOfDay
: (masterStart ?? baseStartLocal).TimeOfDay;
				patternSeriesStart = startDate.Add(timeOfDay);
			}
		}
		catch (COMException)
		{
		}

		var hasPatternSeriesStart = patternSeriesStart.HasValue;
		if (hasPatternSeriesStart && patternSeriesStart.Value.Year < 1900)
		{
			hasPatternSeriesStart = false;
			patternSeriesStart = null;
		}

		var seriesStart = patternSeriesStart ?? masterStart ?? baseStartLocal;

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
				if (!hasPatternSeriesStart)
					seriesStart = masterStart.Value;
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero && apptIsMaster)
		{
			var candidate = baseEndUtc - baseStartUtc;
			if (candidate > TimeSpan.Zero)
			{
				if (!hasPatternSeriesStart)
					seriesStart = baseStartLocal;
				baseDuration = candidate;
			}
		}

		if (baseDuration <= TimeSpan.Zero)
		{
			var candidate = baseEndUtc - baseStartUtc;
			if (candidate > TimeSpan.Zero)
				baseDuration = candidate;
		}

		if (seriesStart == DateTime.MinValue || seriesStart.Year < 1900)

			seriesStart = baseStartLocal;

		var seriesEnd = seriesStart.Add(baseDuration);
		var seriesStartUtc = ConvertFromSourceLocalToUtc(seriesStart, $"series '{appt.Subject}' anchor start");
		var seriesEndUtc = ConvertFromSourceLocalToUtc(seriesEnd, $"series '{appt.Subject}' anchor end");
		var calEvent = new CalendarEvent
		{
			Start = new CalDateTime(seriesStartUtc),
			End = new CalDateTime(seriesEndUtc),
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
					var (exStartLocal, exStartUtc) = NormalizeOutlookTimes(ex.AppointmentItem.Start, ex.AppointmentItem.StartUTC, $"exception '{appt.Subject}' start");
					var (exEndLocal, exEndUtc) = NormalizeOutlookTimes(ex.AppointmentItem.End, ex.AppointmentItem.EndUTC, $"exception '{appt.Subject}' end");

					if (exStartLocal >= from && exStartLocal <= to)
					{
						var exUid = $"outlook-{appt.GlobalAppointmentID}-{exStartLocal:yyyyMMddTHHmmss}";
						results.Add((exUid, exStartLocal, exEndLocal, exStartUtc, exEndUtc));
						_logger.LogInformation("Processed modified occurrence for '{Subject}' at {Start}", appt.Subject, exStartLocal);
					}
				}
			}
			catch { /* skip broken exceptions */ }
		}

		// Evaluate occurrences
		var occurrences = calEvent.GetOccurrences(ConvertFromSourceLocalToUtc(from), ConvertFromSourceLocalToUtc(to));

		foreach (var occ in occurrences)
		{
			var startUtc = DateTime.SpecifyKind(occ.Period.StartTime.AsUtc, DateTimeKind.Utc);
			var endUtc = DateTime.SpecifyKind(occ.Period.EndTime?.AsUtc ?? startUtc.Add(baseDuration), DateTimeKind.Utc);
			var startLocal = ConvertUtcToSourceLocal(startUtc);
			var endLocal = ConvertUtcToSourceLocal(endUtc);
			if (skipDates.Contains(startLocal.Date))
				continue;

			var uid = $"outlook-{appt.GlobalAppointmentID}-{startLocal:yyyyMMddTHHmmss}";
			results.Add((uid, startLocal, endLocal, startUtc, endUtc));
		}

		return results;
}


	private OutlookEventDto EnsureEventConsistency(OutlookEventDto dto, string context)
	{
		var startUtc = dto.StartUtc == DateTime.MinValue
			? ConvertFromSourceLocalToUtc(dto.StartLocal, $"{context} start fallback UTC")
			: DateTime.SpecifyKind(dto.StartUtc, DateTimeKind.Utc);
		var endUtc = dto.EndUtc == DateTime.MinValue
			? ConvertFromSourceLocalToUtc(dto.EndLocal, $"{context} end fallback UTC")
			: DateTime.SpecifyKind(dto.EndUtc, DateTimeKind.Utc);

		var startLocal = DateTime.SpecifyKind(dto.StartLocal, DateTimeKind.Unspecified);
		var expectedStartLocal = ConvertUtcToSourceLocal(startUtc);
		if (Math.Abs((startLocal - expectedStartLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
		{
			_logger.LogWarning("Adjusted start local time for {Context}. Computed {ComputedLocal:o} but found {StoredLocal:o}.", context, expectedStartLocal, startLocal);
			startLocal = expectedStartLocal;
		}

		var endLocal = DateTime.SpecifyKind(dto.EndLocal, DateTimeKind.Unspecified);
		var expectedEndLocal = ConvertUtcToSourceLocal(endUtc);
		if (Math.Abs((endLocal - expectedEndLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
		{
			_logger.LogWarning("Adjusted end local time for {Context}. Computed {ComputedLocal:o} but found {StoredLocal:o}.", context, expectedEndLocal, endLocal);
			endLocal = expectedEndLocal;
		}

		CheckTargetAlignment($"{context} start", startLocal, startUtc);
		CheckTargetAlignment($"{context} end", endLocal, endUtc);

		return dto with { StartLocal = startLocal, EndLocal = endLocal, StartUtc = startUtc, EndUtc = endUtc };
	}

	private (DateTime local, DateTime utc) NormalizeOutlookTimes(DateTime localCandidate, DateTime utcCandidate, string context)
	{
		if (utcCandidate == DateTime.MinValue && localCandidate == DateTime.MinValue)
		{
			_logger.LogWarning("Outlook returned no timestamps for {Context}; leaving values unset.", context);
			return (DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Unspecified), DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc));
		}

		DateTime normalizedUtc;
		if (utcCandidate == DateTime.MinValue)
		{
			normalizedUtc = ConvertFromSourceLocalToUtc(localCandidate, $"{context} fallback UTC");
		}
		else
		{
			normalizedUtc = DateTime.SpecifyKind(utcCandidate, DateTimeKind.Utc);
		}

		var expectedLocal = ConvertUtcToSourceLocal(normalizedUtc);
		DateTime normalizedLocal;
		if (localCandidate == DateTime.MinValue)
		{
			normalizedLocal = expectedLocal;
		}
		else
		{
			var candidateLocal = DateTime.SpecifyKind(localCandidate, DateTimeKind.Unspecified);
			if (Math.Abs((candidateLocal - expectedLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
			{
				_logger.LogWarning("Detected timezone mismatch for {Context}: Outlook local {OutlookLocal:o} differed from computed {ComputedLocal:o}. Using UTC-derived value.", context, candidateLocal, expectedLocal);
				normalizedLocal = expectedLocal;
			}
			else
			{
				normalizedLocal = candidateLocal;
			}
		}

		CheckTargetAlignment(context, normalizedLocal, normalizedUtc);

		return (normalizedLocal, normalizedUtc);
	}
	private DateTime ConvertFromSourceLocalToUtc(DateTime local, string? context = null)
	{
		var unspecifiedLocal = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
		var utc = TimeZoneInfo.ConvertTimeToUtc(unspecifiedLocal, _sourceTimeZone);
		if (!string.IsNullOrEmpty(context))
			CheckTargetAlignment(context, unspecifiedLocal, utc);
		return DateTime.SpecifyKind(utc, DateTimeKind.Utc);
	}

	private DateTime ConvertUtcToSourceLocal(DateTime utc, string? context = null)
	{
		var specifiedUtc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
		var local = TimeZoneInfo.ConvertTimeFromUtc(specifiedUtc, _sourceTimeZone);
		var unspecifiedLocal = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
		if (!string.IsNullOrEmpty(context))
			CheckTargetAlignment(context, unspecifiedLocal, specifiedUtc);
		return unspecifiedLocal;
	}

	private void CheckTargetAlignment(string context, DateTime sourceLocal, DateTime utc)
	{
		var specifiedUtc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
		if (_sourceTimeZone.Id.Equals(_targetTimeZone.Id, StringComparison.OrdinalIgnoreCase))
		{
			var targetLocal = TimeZoneInfo.ConvertTimeFromUtc(specifiedUtc, _targetTimeZone);
			if (Math.Abs((targetLocal - sourceLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
				_logger.LogWarning("Sanity check failed for {Context}: source timezone {SourceZone} local {SourceLocal:o} maps to {TargetLocal:o} in target timezone {TargetZone}.", context, _sourceTimeZone.Id, sourceLocal, targetLocal, _targetTimeZone.Id);
		}
	}

	private TimeZoneInfo ResolveTimeZone(string? timeZoneId, string role)
	{
		if (string.IsNullOrWhiteSpace(timeZoneId))
		{
			_logger.LogInformation("Using local system timezone {TimeZone} for {Role} calendar.", TimeZoneInfo.Local.Id, role);
			return TimeZoneInfo.Local;
		}

		try
		{
			var resolved = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId.Trim());
			_logger.LogInformation("Using configured timezone {TimeZone} for {Role} calendar.", resolved.Id, role);
			return resolved;
		}
		catch (TimeZoneNotFoundException)
		{
			_logger.LogWarning("Configured {Role} timezone '{TimeZoneId}' was not found. Falling back to local timezone {Fallback}.", role, timeZoneId, TimeZoneInfo.Local.Id);
		}
		catch (InvalidTimeZoneException)
		{
			_logger.LogWarning("Configured {Role} timezone '{TimeZoneId}' is invalid. Falling back to local timezone {Fallback}.", role, timeZoneId, TimeZoneInfo.Local.Id);
		}

		return TimeZoneInfo.Local;
	}

private Outlook.Application CreateOutlookApplication(CancellationToken token)
{
		EnsureOutlookProcessReady(token);
		Outlook.Application? application = null;
		COMException? lastServerException = null;
		const int maxAttempts = 3;

		for (var attempt = 1; attempt <= maxAttempts; attempt++)
		{
			token.ThrowIfCancellationRequested();

			application = TryGetRunningOutlookInstance();
			if (application != null)
			{
				_logger.LogDebug("Attached to running Outlook instance.");
				return application;
			}

			try
			{
				_logger.LogDebug("Attempting to create Outlook.Application instance (attempt {Attempt}/{MaxAttempts}).", attempt, maxAttempts);
				application = new Outlook.Application();
				_logger.LogDebug("Created new Outlook.Application instance.");
				return application;
			}
			catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
			{
				lastServerException = ex;
				_logger.LogWarning(ex, "Outlook.Application creation failed with CO_E_SERVER_EXEC_FAILURE, attempt {Attempt}/{MaxAttempts}.", attempt, maxAttempts);
				if (attempt == maxAttempts)
				{
					break;
				}
				DelayWithCancellation(TimeSpan.FromSeconds(5), token);
			}
		}

		application = TryGetRunningOutlookInstance();
		if (application != null)
		{
			_logger.LogDebug("Attached to running Outlook instance after retry failures.");
			return application;
		}

		throw lastServerException ?? new COMException("Failed to create Outlook.Application instance.", unchecked((int)0x80080005));
	}

	private Outlook.Application? TryGetRunningOutlookInstance()
	{
		try
		{
			var clsid = OutlookApplicationClsid;
			var hr = GetActiveObjectNative(ref clsid, IntPtr.Zero, out var activeObject);
			if (hr < 0)
			{
				Marshal.ThrowExceptionForHR(hr);
			}
			if (activeObject is Outlook.Application outlookApp)
			{
				return outlookApp;
			}
			_logger.LogDebug("Active Outlook COM object was not of the expected type.");
			return null;
		}
		catch (COMException ex) when (ex.HResult == unchecked((int)0x800401E3) || ex.HResult == unchecked((int)0x80040154))
		{
			return null;
		}
		catch (Exception ex)
		{
			_logger.LogDebug(ex, "Unable to attach to existing Outlook instance.");
			return null;
		}
	}

	private void EnsureOutlookProcessReady(CancellationToken token)
	{
		try
		{
			var processes = Process.GetProcessesByName("OUTLOOK");
			if (processes.Length == 0)
			{
				_logger.LogWarning("Outlook process not detected. Attempting to start outlook.exe in background mode.");
				var resolvedPath = ResolveOutlookExecutablePath();
				var useShellExecute = resolvedPath == null;
				var executable = resolvedPath ?? "outlook.exe";

				var startInfo = new ProcessStartInfo(executable)
				{
					UseShellExecute = useShellExecute,
					Arguments = "/embedding",
					WindowStyle = ProcessWindowStyle.Minimized
				};

				if (useShellExecute)
				{
					_logger.LogDebug("Starting Outlook via shell.");
				}
				else
				{
					_logger.LogDebug("Starting Outlook using resolved path '{Executable}'.", executable);
				}

				try
				{
					Process.Start(startInfo);
				}
				catch (Exception ex)
				{
					_logger.LogWarning(ex, "Unable to start Outlook using '{Executable}'.", executable);
				}
			}

			var wait = Stopwatch.StartNew();
			while (Process.GetProcessesByName("OUTLOOK").Length == 0 && wait.Elapsed < TimeSpan.FromSeconds(30))
			{
				DelayWithCancellation(TimeSpan.FromSeconds(1), token);
			}

			if (Process.GetProcessesByName("OUTLOOK").Length == 0)
			{
				_logger.LogWarning("Outlook process could not be detected after attempting to start it. Ensure Outlook is installed and registered correctly.");
				return;
			}

			if (wait.Elapsed < TimeSpan.FromSeconds(30))
			{
				DelayWithCancellation(TimeSpan.FromSeconds(2), token);
			}
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogWarning(ex, "Failed to ensure Outlook process is running.");
		}
	}

	private string? ResolveOutlookExecutablePath()
	{
		if (!OperatingSystem.IsWindows())
		{
			return null;
		}

		try
		{
			using var key = Registry.LocalMachine.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\App Paths\\OUTLOOK.EXE");
			var value = key?.GetValue(string.Empty) as string;
			if (string.IsNullOrWhiteSpace(value))
			{
				return null;
			}

			if (File.Exists(value))
			{
				return value;
			}

			_logger.LogWarning("Outlook executable path '{Path}' from registry does not exist.", value);
		}
		catch (Exception ex)
		{
			_logger.LogDebug(ex, "Unable to read Outlook executable path from registry.");
		}

		return null;
	}

	private static void DelayWithCancellation(TimeSpan delay, CancellationToken token)
	{
		if (delay <= TimeSpan.Zero)
		{
			return;
		}

		var waitHandles = new[] { token.WaitHandle };
		if (WaitHandle.WaitAny(waitHandles, delay) == WaitHandle.WaitTimeout)
		{
			return;
		}

		token.ThrowIfCancellationRequested();
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