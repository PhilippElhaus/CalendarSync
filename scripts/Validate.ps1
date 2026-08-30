[CmdletBinding()]
param(
	[switch]$SkipVulnerabilityAudit
)

$ErrorActionPreference = 'Stop'
$repo = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if ([System.IO.Path]::GetPathRoot($tempRoot).Equals('D:\', [StringComparison]::OrdinalIgnoreCase)) {
	throw 'Validation output cannot use D: as its temporary root.'
}

$output = Join-Path $tempRoot "CalendarSync.Validation.$([Guid]::NewGuid().ToString('N'))"
$resolvedOutput = [System.IO.Path]::GetFullPath($output)
$expectedPrefix = $tempRoot.TrimEnd('\') + '\CalendarSync.Validation.'
if (-not $resolvedOutput.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
	throw "Unsafe validation output path: $resolvedOutput"
}

try {
	dotnet restore (Join-Path $repo 'CalendarSync.sln') --locked-mode
	if ($LASTEXITCODE -ne 0) { throw 'Locked restore failed.' }
	if (-not $SkipVulnerabilityAudit) {
		$auditOutput = & dotnet list (Join-Path $repo 'CalendarSync.sln') package --vulnerable --include-transitive --no-restore 2>&1
		$auditOutput | Write-Output
		if ($LASTEXITCODE -ne 0) { throw 'NuGet vulnerability audit failed to run.' }
		if ($auditOutput -match 'has the following vulnerable packages') {
			throw 'NuGet vulnerability audit found a vulnerable package.'
		}
	}

	$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
	if (-not (Test-Path -LiteralPath $vswhere)) {
		throw 'Visual Studio Installer vswhere.exe is required to locate full MSBuild for the Outlook COM reference.'
	}
	$msbuild = & $vswhere -latest -products * -requires Microsoft.Component.MSBuild -find 'MSBuild\**\Bin\MSBuild.exe' | Select-Object -First 1
	if ([string]::IsNullOrWhiteSpace($msbuild)) {
		throw 'Full Visual Studio MSBuild is required for the Outlook COM reference.'
	}

	& $msbuild (Join-Path $repo 'CalendarSync.Tests\CalendarSync.Tests.csproj') `
		/t:Build `
		/p:Configuration=Release `
		/p:RestoreLockedMode=true `
		/warnaserror `
		/m `
		/v:minimal
	if ($LASTEXITCODE -ne 0) { throw 'Test build failed.' }

	$testAssembly = Join-Path $repo 'CalendarSync.Tests\bin\Release\net8.0-windows\CalendarSync.Tests.dll'
	dotnet vstest $testAssembly /Platform:x86
	if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }

	& $msbuild (Join-Path $repo 'CalendarSync.csproj') `
		/t:Build `
		/p:Configuration=Release `
		/p:RestoreLockedMode=true `
		"/p:OutputPath=$resolvedOutput\" `
		/warnaserror `
		/m `
		/v:minimal
	if ($LASTEXITCODE -ne 0) { throw 'Release package build failed.' }

	& (Join-Path $PSScriptRoot 'Test-Package.ps1') -OutputPath $resolvedOutput
}
finally {
	if (Test-Path -LiteralPath $resolvedOutput) {
		$finalOutput = [System.IO.Path]::GetFullPath($resolvedOutput)
		if ($finalOutput.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
			Remove-Item -LiteralPath $finalOutput -Recurse -Force
		}
	}
}
