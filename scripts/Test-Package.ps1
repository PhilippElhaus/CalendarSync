[CmdletBinding()]
param(
	[Parameter(Mandatory)]
	[string]$OutputPath
)

$ErrorActionPreference = 'Stop'
$resolvedOutput = [System.IO.Path]::GetFullPath($OutputPath)
if (-not (Test-Path -LiteralPath $resolvedOutput -PathType Container)) {
	throw "Package output does not exist: $resolvedOutput"
}

$requiredRootFiles = @(
	'CalendarSync.exe',
	'CalendarSync.dll',
	'CalendarSync.deps.json',
	'CalendarSync.runtimeconfig.json',
	'config.example.json',
	'Readme.txt'
)
$requiredBinFiles = @(
	'Ical.Net.dll',
	'Newtonsoft.Json.dll',
	'NodaTime.dll',
	'Serilog.dll',
	'Serilog.Extensions.Logging.dll',
	'System.Diagnostics.EventLog.dll'
)
$requiredIcons = @('cal64.ico', 'icon_idle.ico', 'icon_update.ico', 'icon_delete.ico')

foreach ($file in $requiredRootFiles) {
	if (-not (Test-Path -LiteralPath (Join-Path $resolvedOutput $file) -PathType Leaf)) {
		throw "Required package file is missing: $file"
	}
}

foreach ($file in $requiredBinFiles) {
	if (-not (Test-Path -LiteralPath (Join-Path $resolvedOutput "bin\$file") -PathType Leaf)) {
		throw "Required relocated dependency is missing: bin\$file"
	}
}

foreach ($file in $requiredIcons) {
	if (-not (Test-Path -LiteralPath (Join-Path $resolvedOutput "ico\$file") -PathType Leaf)) {
		throw "Required icon is missing: ico\$file"
	}
}

if (Test-Path -LiteralPath (Join-Path $resolvedOutput 'config.json')) {
	throw 'A clean package must not contain config.json. It must contain config.example.json only.'
}

if (Test-Path -LiteralPath (Join-Path $resolvedOutput 'runtimes')) {
	throw 'The package contains an unexpected runtimes directory after DLL relocation.'
}

function Get-PeMachine([string]$Path) {
	$stream = [System.IO.File]::OpenRead($Path)
	try {
		$reader = [System.IO.BinaryReader]::new($stream)
		$stream.Position = 0x3c
		$peOffset = $reader.ReadInt32()
		$stream.Position = $peOffset + 4
		return $reader.ReadUInt16()
	}
	finally {
		$stream.Dispose()
	}
}

$i386Machine = 0x014c
foreach ($binary in @('CalendarSync.exe', 'CalendarSync.dll')) {
	$machine = Get-PeMachine (Join-Path $resolvedOutput $binary)
	if ($machine -ne $i386Machine) {
		throw "$binary is not an x86 PE image. Machine=0x$($machine.ToString('X4'))"
	}
}

$tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if ([System.IO.Path]::GetPathRoot($tempRoot).Equals('D:\', [StringComparison]::OrdinalIgnoreCase)) {
	throw 'Package smoke staging cannot use D: as its temporary root.'
}

$stage = Join-Path $tempRoot "CalendarSync.PackageSmoke.$([Guid]::NewGuid().ToString('N'))"
$resolvedStage = [System.IO.Path]::GetFullPath($stage)
$expectedPrefix = $tempRoot.TrimEnd('\') + '\CalendarSync.PackageSmoke.'
if (-not $resolvedStage.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
	throw "Unsafe package smoke staging path: $resolvedStage"
}

try {
	New-Item -ItemType Directory -Path $resolvedStage | Out-Null
	Get-ChildItem -LiteralPath $resolvedOutput -Force | ForEach-Object {
		Copy-Item -LiteralPath $_.FullName -Destination $resolvedStage -Recurse -Force
	}

	$process = Start-Process `
		-FilePath (Join-Path $resolvedStage 'CalendarSync.exe') `
		-ArgumentList '--self-test' `
		-WorkingDirectory $resolvedStage `
		-WindowStyle Hidden `
		-Wait `
		-PassThru
	try {
		if ($process.ExitCode -ne 0) {
			throw "Package self-test failed with exit code $($process.ExitCode)."
		}
	}
	finally {
		$process.Dispose()
	}
}
finally {
	if (Test-Path -LiteralPath $resolvedStage) {
		$finalStage = [System.IO.Path]::GetFullPath($resolvedStage)
		if ($finalStage.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
			Remove-Item -LiteralPath $finalStage -Recurse -Force
		}
	}
}

Write-Output "CalendarSync package smoke test passed: $resolvedOutput"
