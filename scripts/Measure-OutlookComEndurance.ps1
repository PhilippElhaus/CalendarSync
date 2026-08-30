[CmdletBinding()]
param(
	[ValidateRange(5, 240)]
	[int]$DurationMinutes = 15,

	[ValidateRange(5, 300)]
	[int]$SampleIntervalSeconds = 15,

	[switch]$AcknowledgeLiveOutlook
)

$ErrorActionPreference = 'Stop'
if (-not $AcknowledgeLiveOutlook) {
	throw 'This check observes the live CalendarSync and Outlook processes. Re-run with -AcknowledgeLiveOutlook.'
}

$calendarSync = Get-Process -Name 'CalendarSync' -ErrorAction Stop | Select-Object -First 1
$calendarSyncId = $calendarSync.Id
$calendarSync.Dispose()
$samples = [System.Collections.Generic.List[object]]::new()
$deadline = [DateTime]::UtcNow.AddMinutes($DurationMinutes)

while ([DateTime]::UtcNow -lt $deadline) {
	$syncProcess = Get-Process -Id $calendarSyncId -ErrorAction Stop
	$outlookProcesses = @(Get-Process -Name 'OUTLOOK' -ErrorAction SilentlyContinue)
	try {
		$samples.Add([PSCustomObject]@{
			TimestampUtc = [DateTime]::UtcNow
			SyncHandles = $syncProcess.HandleCount
			SyncThreads = $syncProcess.Threads.Count
			SyncPrivateBytes = $syncProcess.PrivateMemorySize64
			OutlookHandles = ($outlookProcesses | Measure-Object -Property HandleCount -Sum).Sum
			OutlookPrivateBytes = ($outlookProcesses | Measure-Object -Property PrivateMemorySize64 -Sum).Sum
		})
	}
	finally {
		$syncProcess.Dispose()
		$outlookProcesses | ForEach-Object { $_.Dispose() }
	}

	Start-Sleep -Seconds $SampleIntervalSeconds
}

if ($samples.Count -lt 6) {
	throw 'The endurance check collected too few samples.'
}

$windowSize = [Math]::Max(2, [Math]::Floor($samples.Count / 3))
$first = $samples | Select-Object -First $windowSize
$last = $samples | Select-Object -Last $windowSize

function Get-Average($Items, [string]$Property) {
	return [double](($Items | Measure-Object -Property $Property -Average).Average)
}

$handleGrowth = (Get-Average $last 'SyncHandles') - (Get-Average $first 'SyncHandles')
$threadGrowth = (Get-Average $last 'SyncThreads') - (Get-Average $first 'SyncThreads')
$privateByteGrowth = (Get-Average $last 'SyncPrivateBytes') - (Get-Average $first 'SyncPrivateBytes')

$summary = [PSCustomObject]@{
	Samples = $samples.Count
	DurationMinutes = $DurationMinutes
	SyncHandleGrowth = [Math]::Round($handleGrowth, 2)
	SyncThreadGrowth = [Math]::Round($threadGrowth, 2)
	SyncPrivateByteGrowthMiB = [Math]::Round($privateByteGrowth / 1MB, 2)
	PeakSyncHandles = ($samples | Measure-Object -Property SyncHandles -Maximum).Maximum
	PeakSyncThreads = ($samples | Measure-Object -Property SyncThreads -Maximum).Maximum
}
$summary

if ($handleGrowth -gt 10 -or $threadGrowth -gt 3 -or $privateByteGrowth -gt 64MB) {
	throw 'CalendarSync resource use did not reach a stable plateau. Review sync.log and Outlook behavior.'
}

Write-Output 'Outlook COM endurance check passed.'
