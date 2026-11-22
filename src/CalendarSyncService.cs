using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CalendarSync;

public partial class CalendarSyncService : BackgroundService
{
	internal enum SyncCycleOutcome
	{
		Succeeded,
		Failed,
		Canceled,
		Skipped
	}

	private readonly SyncConfig _config;
	private readonly ILogger _logger;
	private readonly TrayIconManager? _tray;
	private readonly Func<CancellationToken, Task<OutlookSnapshot>> _readSnapshot;
	private readonly Func<HttpClient> _createClient;
	private readonly Func<TimeSpan, CancellationToken, Task> _wipeDelay;
	private readonly TimeSpan _initialWait;
	private readonly TimeSpan _syncInterval;
	private readonly string _sourceId;
	private readonly string? _tag;
	private readonly TimeZoneInfo _sourceTimeZone;
	private readonly TimeZoneInfo _targetTimeZone;
	private readonly SemaphoreSlim _opLock = new(1, 1);
	private readonly SemaphoreSlim _outlookComGate = new(1, 1);
	private readonly object _operationSync = new();
	private CancellationTokenSource? _currentOpCts;
	private CancellationToken _serviceStoppingToken = CancellationToken.None;
	private DateTimeOffset? _lastSuccessfulSync;
	private readonly CalDavAuthenticationNotice _authenticationNotice = new();
	private bool _hadSuccessfulCalDavRequest;
	private static readonly Guid OutlookApplicationClsid = new("0006F03A-0000-0000-C000-000000000046");

	[DllImport("oleaut32.dll", EntryPoint = "GetActiveObject")]
	private static extern int GetActiveObject(ref Guid rclsid, IntPtr pvReserved, [MarshalAs(UnmanagedType.IUnknown)] out object? ppunk);

	public CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger, TrayIconManager tray)
		: this(config, logger, null, null, tray ?? throw new ArgumentNullException(nameof(tray)))
	{
	}

	internal CalendarSyncService(SyncConfig config, ILogger<CalendarSyncService> logger,
		Func<CancellationToken, Task<OutlookSnapshot>>? readSnapshot, Func<HttpClient>? createClient, TrayIconManager? tray = null,
		Func<TimeSpan, CancellationToken, Task>? wipeDelay = null)
	{
		_config = config ?? throw new ArgumentNullException(nameof(config));
		_logger = new BestEffortLogger(logger ?? throw new ArgumentNullException(nameof(logger)));
		_tray = tray;
		_readSnapshot = readSnapshot ?? FetchOutlookEventsAsync;
		_createClient = createClient ?? CreateHttpClient;
		_wipeDelay = wipeDelay ?? Task.Delay;
		_initialWait = TimeSpan.FromSeconds(_config.InitialWaitSeconds);
		_syncInterval = TimeSpan.FromMinutes(_config.SyncIntervalMinutes);
		_sourceId = _config.SourceId ?? string.Empty;
		_tag = string.IsNullOrWhiteSpace(_config.EventTag) ? null : _config.EventTag.Trim();
		_sourceTimeZone = ResolveTimeZone(_config.SourceTimeZoneId, "source");
		_targetTimeZone = ResolveTimeZone(_config.TargetTimeZoneId, "target");

		if (!_sourceTimeZone.Id.Equals(_targetTimeZone.Id, StringComparison.OrdinalIgnoreCase))
		{
			_logger.LogInformation("Configured source and target time zones differ; timed events use UTC.");
		}
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		_serviceStoppingToken = stoppingToken;
		_logger.LogInformation("Calendar Sync Service started.");
		EventRecorder.WriteEntry("Service started", EventLogEntryType.Information);

		try
		{
			_logger.LogInformation("Initial wait for {InitialWait} seconds before starting sync.", _initialWait.TotalSeconds);
			await Task.Delay(_initialWait, stoppingToken).ConfigureAwait(false);

			while (!stoppingToken.IsCancellationRequested)
			{
				await _opLock.WaitAsync(stoppingToken).ConfigureAwait(false);
				using var operationCts = BeginOperation();
				using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, operationCts.Token);

				try
				{
					await PerformSyncAsync(linkedCts.Token, false).ConfigureAwait(false);
				}
				finally
				{
					EndOperation(operationCts);
					_opLock.Release();
				}

				_logger.LogDebug("Waiting for next sync cycle.");
				await Task.Delay(_syncInterval, stoppingToken).ConfigureAwait(false);
			}
		}
		catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
		{
			_logger.LogInformation("Calendar Sync Service cancellation observed.");
		}
		finally
		{
			_logger.LogInformation("Calendar Sync Service stopped.");
			EventRecorder.WriteEntry("Service stopped", EventLogEntryType.Information);
		}
	}

	internal async Task<SyncCycleOutcome> PerformSyncAsync(CancellationToken token, bool fullResync)
	{
		var cycleId = Guid.NewGuid().ToString("N")[..8];
		var stopwatch = Stopwatch.StartNew();
		var sourceEventCount = 0;
		var outcome = SyncCycleOutcome.Failed;
		var metrics = new CycleMetrics();
		var reason = "None";
		int? httpStatus = null;
		HttpStatusCode? accessFailureStatus = null;
		_hadSuccessfulCalDavRequest = false;
		using var scope = _logger.BeginScope(new Dictionary<string, object> { ["SyncCycleId"] = cycleId });

		EventRecorder.WriteEntry(fullResync ? "Full re-sync started" : "Sync started", EventLogEntryType.Information);
		_tray?.SetUpdating();
		_logger.LogInformation("Starting {SyncKind} cycle {CycleId}.", fullResync ? "full re-sync" : "sync", cycleId);

		try
		{
			var snapshot = await _readSnapshot(token).ConfigureAwait(false);
			token.ThrowIfCancellationRequested();
			sourceEventCount = snapshot.Events.Count;
			metrics.FailedItems = snapshot.FailedItemCount;
			metrics.HitItemLimit = snapshot.HitItemLimit;
			metrics.BodyNotRead = snapshot.Events.Values.Count(dto => !dto.BodyWasRead);

			if (!snapshot.IsComplete)
			{
				reason = "SnapshotIncomplete";
				_logger.LogError(
					"Outlook snapshot is incomplete. FailedItems={FailedItems}, HitItemLimit={HitItemLimit}. No iCloud changes were made.",
					snapshot.FailedItemCount,
					snapshot.HitItemLimit);
				EventRecorder.WriteEntry("Outlook snapshot incomplete; iCloud mutation skipped", EventLogEntryType.Error);
				_tray?.SetFailed("Outlook read incomplete; retry pending");
				return SyncCycleOutcome.Failed;
			}

			await ApplySnapshotToICloudAsync(snapshot.Events, fullResync, metrics, token).ConfigureAwait(false);

			_lastSuccessfulSync = DateTimeOffset.Now;
			outcome = SyncCycleOutcome.Succeeded;
			EventRecorder.WriteEntry(fullResync ? "Full re-sync finished" : "Sync finished", EventLogEntryType.Information);
			_tray?.SetIdle($"Idle - last sync {_lastSuccessfulSync:HH:mm}");
			return outcome;
		}
		catch (CalDavAccessException ex)
		{
			accessFailureStatus = ex.StatusCode;
			reason = "AccessRejected";
			httpStatus = (int)ex.StatusCode!.Value;
			_logger.LogError(
				"iCloud rejected {Operation} with HTTP {Status}. This cycle stopped; the next cycle will retry.",
				ex.Operation, (int)ex.StatusCode!.Value);
			EventRecorder.WriteEntry($"iCloud request rejected with HTTP {(int)ex.StatusCode.Value}", EventLogEntryType.Error);
			_tray?.SetFailed($"iCloud HTTP {(int)ex.StatusCode.Value}; retry pending");
			return SyncCycleOutcome.Failed;
		}
		catch (OutlookOperationInProgressException)
		{
			outcome = SyncCycleOutcome.Skipped;
			reason = "OutlookBusy";
			_logger.LogWarning("Previous Outlook operation is still running. Skipping this sync cycle.");
			_tray?.SetFailed("Outlook is still busy; retry pending");
			return outcome;
		}
		catch (OperationCanceledException)
		{
			outcome = SyncCycleOutcome.Canceled;
			if (_serviceStoppingToken.IsCancellationRequested)
			{
				reason = "Stopping";
				_logger.LogInformation("Sync canceled because the service is stopping.");
			}
			else if (IsCurrentOperationCanceled())
			{
				reason = "ManualRequest";
				_logger.LogInformation("Sync canceled in preparation for a manual full re-sync.");
			}
			else if (!token.IsCancellationRequested)
			{
				outcome = SyncCycleOutcome.Failed;
				reason = "OutlookTimeout";
				_logger.LogError("Outlook operation timed out.");
				EventRecorder.WriteEntry("Outlook operation timed out", EventLogEntryType.Error);
				_tray?.SetFailed("Outlook timed out; retry pending");
			}
			else
			{
				reason = "Canceled";
			}

			return outcome;
		}
		catch (Exception ex)
		{
			var requestError = ex as HttpRequestException;
			httpStatus = (int?)(requestError?.StatusCode ?? (ex as CalDavDataException)?.StatusCode);
			reason = ex switch
			{
				CalDavDataException error => error.Reason,
				CalDavTimeoutException => "CalDavTimeout",
				HttpRequestException { StatusCode: HttpStatusCode.PreconditionFailed } => "PreconditionFailed",
				HttpRequestException { StatusCode: not null } when metrics.HasPossibleMutations => "HttpMutationUnconfirmed",
				HttpRequestException { StatusCode: not null } => "HttpRejected",
				HttpRequestException => "TransportOrResponseBuffer",
				COMException => "OutlookComFailure",
				_ => "UnexpectedFailure"
			};
			_logger.LogError("Sync stopped at {Stage}: {Reason}; HTTP status={HttpStatus}.", metrics.Stage, reason, httpStatus);
			EventRecorder.WriteEntry("Sync failed", EventLogEntryType.Error);
			_tray?.SetFailed("Last sync failed; see log");
			return SyncCycleOutcome.Failed;
		}
		finally
		{
			stopwatch.Stop();
			if (_authenticationNotice.RecordCycle(accessFailureStatus, _hadSuccessfulCalDavRequest, DateTimeOffset.UtcNow))
			{
				_tray?.ShowError("CalendarSync",
					"iCloud repeatedly rejected authentication (HTTP 401). " +
					"No iCloud request succeeded in these cycles. Check the account and app-specific password if this continues.");
			}

			var mutationOutcome = metrics.GetMutationOutcome(outcome);
			_logger.Log(outcome == SyncCycleOutcome.Failed || mutationOutcome is "Partial" or "Possible" ? LogLevel.Warning : LogLevel.Information,
				"SyncCycle Schema={Schema} CycleId={CycleId} FullResync={FullResync} Outcome={Outcome} Stage={Stage} Reason={Reason} HttpStatus={HttpStatus} ElapsedMs={ElapsedMs} SourceEvents={SourceEvents} FailedItems={FailedItems} HitItemLimit={HitItemLimit} BodyNotRead={BodyNotRead} Listed={Listed} Unchanged={Unchanged} Written={Written} Verified={Verified} StaleCandidates={StaleCandidates} OwnershipSkipped={OwnershipSkipped} Deleted={Deleted} WipeListed={WipeListed} WipeDeleted={WipeDeleted} MutationOutcome={MutationOutcome} WritePossible={WritePossible} DeletePossible={DeletePossible} WipeDeletePossible={WipeDeletePossible} WipeRemaining={WipeRemaining}.",
				2, cycleId, fullResync, outcome, metrics.Stage, reason, httpStatus, stopwatch.ElapsedMilliseconds,
				sourceEventCount, metrics.FailedItems, metrics.HitItemLimit, metrics.BodyNotRead, metrics.Listed, metrics.Unchanged, metrics.Writes.Confirmed,
				metrics.Verified, metrics.StaleCandidates, metrics.OwnershipSkipped, metrics.Deletes.Confirmed, metrics.WipeListed, metrics.WipeDeletes.Confirmed,
				mutationOutcome, metrics.Writes.Possible, metrics.Deletes.Possible, metrics.WipeDeletes.Possible, metrics.WipeRemaining);
		}
	}

	private async Task ApplySnapshotToICloudAsync(
		Dictionary<string, OutlookEventDto> outlookEvents,
		bool fullResync,
		CycleMetrics metrics,
		CancellationToken token)
	{
		using var client = _createClient();
		var calendarUri = new Uri(BuildCalendarUrl());
		Dictionary<string, RemoteResource> remote;

		if (fullResync)
		{
			_logger.LogInformation("Complete Outlook snapshot obtained. Starting requested full iCloud wipe.");
			remote = await WipeICloudCalendarAsync(client, calendarUri, metrics, token).ConfigureAwait(false);
		}
		else
		{
			metrics.Stage = "Listing";
			remote = await GetICloudEventsAsync(client, calendarUri, _sourceId, token).ConfigureAwait(false);
		}

		await SyncWithICloudAsync(client, calendarUri, outlookEvents, remote, metrics, token).ConfigureAwait(false);
		metrics.Stage = "Complete";
	}

	private sealed class CycleMetrics
	{
		internal string Stage = "Snapshot";
		internal int FailedItems;
		internal bool HitItemLimit;
		internal int BodyNotRead;
		internal int Listed;
		internal int Unchanged;
		internal readonly MutationCounts Writes = new();
		internal int Verified;
		internal int StaleCandidates;
		internal int OwnershipSkipped;
		internal readonly MutationCounts Deletes = new();
		internal int WipeListed;
		internal readonly MutationCounts WipeDeletes = new();
		internal int? WipeRemaining;
		internal bool HasPossibleMutations => Writes.Possible + Deletes.Possible + WipeDeletes.Possible > 0;

		internal string GetMutationOutcome(SyncCycleOutcome outcome) =>
			Writes.Confirmed + Deletes.Confirmed + WipeDeletes.Confirmed > 0
				? outcome == SyncCycleOutcome.Succeeded ? "Confirmed" : "Partial"
				: HasPossibleMutations ? "Possible" : "None";
	}

	private sealed class MutationCounts
	{
		internal int Confirmed;
		internal int Possible;
	}

	public async Task TriggerFullResyncAsync()
	{
		EventRecorder.WriteEntry("Manual full re-sync requested", EventLogEntryType.Information);
		CancelCurrentOperation();

		try
		{
			await _opLock.WaitAsync(_serviceStoppingToken).ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (_serviceStoppingToken.IsCancellationRequested)
		{
			return;
		}

		using var operationCts = BeginOperation();
		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(operationCts.Token, _serviceStoppingToken);

		try
		{
			await PerformSyncAsync(linkedCts.Token, true).ConfigureAwait(false);
		}
		finally
		{
			EndOperation(operationCts);
			_opLock.Release();
		}
	}

	private string BuildCalendarUrl() =>
		$"{_config.ICloudCalDavUrl!.TrimEnd('/')}/{_config.PrincipalId}/calendars/{_config.WorkCalendarId}/";

	private CancellationTokenSource BeginOperation()
	{
		var cts = new CancellationTokenSource();
		lock (_operationSync)
		{
			_currentOpCts = cts;
		}

		return cts;
	}

	private void EndOperation(CancellationTokenSource cts)
	{
		lock (_operationSync)
		{
			if (ReferenceEquals(_currentOpCts, cts))
			{
				_currentOpCts = null;
			}
		}
	}

	private void CancelCurrentOperation()
	{
		lock (_operationSync)
		{
			_currentOpCts?.Cancel();
		}
	}

	private bool IsCurrentOperationCanceled()
	{
		lock (_operationSync)
		{
			return _currentOpCts?.IsCancellationRequested == true;
		}
	}

	private async Task<HttpResponseMessage> SendCalDavAsync(
		HttpClient client,
		Func<HttpRequestMessage> requestFactory,
		string operation,
		CancellationToken token,
		bool retryVerificationAccess = false,
		Action? markMutationPossible = null)
	{
		var response = await CalDavRequests.SendAsync(
			client, requestFactory, operation, token, _logger, retryVerificationAccess, markMutationPossible).ConfigureAwait(false);
		_hadSuccessfulCalDavRequest = true;
		return response;
	}

	private async Task<HttpResponseMessage> SendMutationAsync(HttpClient client, Func<HttpRequestMessage> requestFactory,
		string operation, MutationCounts counts, CancellationToken token, bool isUpsert = false)
	{
		var response = await SendCalDavAsync(client, requestFactory, operation, token,
			markMutationPossible: () => counts.Possible++).ConfigureAwait(false);
		var status = response.StatusCode;
		if (status is HttpStatusCode.NoContent or HttpStatusCode.OK || (isUpsert && status == HttpStatusCode.Created))
		{
			counts.Confirmed++;
			return response;
		}

		response.Dispose();
		counts.Possible++;
		throw new CalDavDataException(isUpsert ? "WriteNotConfirmed" : "DeleteNotConfirmed", status);
	}
}
