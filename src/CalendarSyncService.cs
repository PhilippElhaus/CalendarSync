using System.Diagnostics;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CalendarSync;

public partial class CalendarSyncService : BackgroundService
{
	private record OutlookEventDto(
	string Subject,
	string Body,
	string Location,
	DateTime StartLocal,
	DateTime EndLocal,
	DateTime StartUtc,
	DateTime EndUtc,
	string GlobalId,
	bool IsAllDay
	);

	private readonly SyncConfig _config;
	private readonly ILogger<CalendarSyncService> _logger;
	private readonly TrayIconManager _tray;
	private static bool _isFirstRun = true;
	private const double TimezoneSanityToleranceMinutes = 1;
	private const double AllDayToleranceMinutes = 5;
	private readonly TimeSpan _initialWait;
	private readonly TimeSpan _syncInterval;
	private readonly string _sourceId;
	private readonly string? _tag;
	private readonly TimeZoneInfo _sourceTimeZone;
	private readonly TimeZoneInfo _targetTimeZone;
	private readonly SemaphoreSlim _opLock = new(1, 1);
	private CancellationTokenSource _currentOpCts = new();
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
		_sourceId = _config.SourceId ?? string.Empty;
		_tag = string.IsNullOrWhiteSpace(_config.EventTag) ? null : _config.EventTag.Trim();
		_sourceTimeZone = ResolveTimeZone(_config.SourceTimeZoneId, "source");
		_targetTimeZone = ResolveTimeZone(_config.TargetTimeZoneId, "target");

		if (!_sourceTimeZone.Id.Equals(_targetTimeZone.Id, StringComparison.OrdinalIgnoreCase))
		{
			_logger.LogInformation(
			"Source timezone {Source} and target timezone {Target} differ; synchronizing via UTC conversions.",
			_sourceTimeZone.Id,
			_targetTimeZone.Id);
		}
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
}
