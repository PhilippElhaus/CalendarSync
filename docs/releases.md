# Windows release and deployment

Use PowerShell 7 on a Windows workstation with Outlook interop, the .NET 8
SDK, and full Visual Studio MSBuild. Run the repository checks first:

```powershell
.\scripts\Validate.ps1
```

The validation script removes its temporary package. Commit the validated
source before building the delivery package. This keeps its embedded source
revision equal to the release commit. Release `v2.3` uses build version `2.3.0`.

## Build the delivery package

Run these commands from the repository. Keep the package under the Windows
temporary directory. Do not use `D:` for release staging.

```powershell
$ErrorActionPreference = 'Stop'
$releaseVersion = '2.3.0'
$releaseTag = 'v2.3'
$releaseCommit = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'Cannot identify the release commit.' }
$releaseTempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if ([System.IO.Path]::GetPathRoot($releaseTempRoot) -eq 'D:\') {
    throw 'Release staging cannot use D:.'
}
$releaseStage = Join-Path $releaseTempRoot "CalendarSync.Release.$([Guid]::NewGuid().ToString('N'))"
$releasePackage = Join-Path $releaseStage 'package'
New-Item -ItemType Directory -Path $releasePackage | Out-Null
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
$releaseMsbuild = & $vswhere -latest -products * -requires Microsoft.Component.MSBuild `
    -find 'MSBuild\**\Bin\MSBuild.exe' | Select-Object -First 1
if ([string]::IsNullOrWhiteSpace($releaseMsbuild)) { throw 'Full MSBuild is required.' }
& $releaseMsbuild .\CalendarSync.csproj /t:Build /p:Configuration=Release `
    /p:RestoreLockedMode=true /p:PlatformTarget=x86 "/p:Version=$releaseVersion" `
    "/p:SourceRevisionId=$releaseCommit" `
    "/p:AssemblyVersion=$releaseVersion.0" "/p:FileVersion=$releaseVersion.0" `
    "/p:OutputPath=$releasePackage\" /warnaserror /m /v:minimal
if ($LASTEXITCODE -ne 0) { throw 'Release build failed.' }
.\scripts\Test-Package.ps1 -OutputPath $releasePackage
```

`Test-Package.ps1` checks the dependency layout and x86 binaries. It runs the
package self-test in a separate temporary directory. It does not read deployed
configuration or contact Outlook or iCloud.

Inspect the package before release. Include `config.example.json`. Exclude
deployed `config.json`, logs, credentials, and runtime data. Create a ZIP and
its SHA-256 checksum:

```powershell
$releasePrivateFiles = @(Get-ChildItem -LiteralPath $releasePackage -Recurse -File |
    Where-Object { $_.Name -eq 'config.json' -or $_.Extension -eq '.log' })
if ($releasePrivateFiles.Count -ne 0) { throw 'Package contains configuration or logs.' }
$releaseZip = Join-Path $releaseStage "CalendarSync-$releaseTag-win-x86.zip"
Compress-Archive -Path (Join-Path $releasePackage '*') -DestinationPath $releaseZip
$releaseChecksum = "$releaseZip.sha256"
$releaseHash = (Get-FileHash -LiteralPath $releaseZip -Algorithm SHA256).Hash.ToLowerInvariant()
"$releaseHash  $([System.IO.Path]::GetFileName($releaseZip))" |
    Set-Content -LiteralPath $releaseChecksum -Encoding ascii
```

## Publish the release

Push the validated commit to `main` without force. Record
the exact commit in the release notes. Create and push the release tag for
that commit. Use the authenticated Windows GitHub CLI:

```powershell
$releaseCommit = git rev-parse HEAD
git tag $releaseTag $releaseCommit
if ($LASTEXITCODE -ne 0) { throw 'Release tag creation failed.' }
git push origin $releaseTag
if ($LASTEXITCODE -ne 0) { throw 'Release tag push failed.' }
$releaseNotes = Join-Path $releaseStage 'release-notes.md'
```

Write the release notes to `$releaseNotes` before publication. Describe the
fix, validation, build version, exact commit, and rollback. Do not include
configuration or calendar data. Then publish the package:

```powershell
gh release create $releaseTag $releaseZip $releaseChecksum `
    --repo PhilippElhaus/CalendarSync --verify-tag `
    --title "$releaseTag Release" --notes-file $releaseNotes
if ($LASTEXITCODE -ne 0) { throw 'Release publication failed.' }
```

Download the published ZIP and checksum into a new temporary directory.
Confirm that its SHA-256 hash matches both the published checksum and
`$releaseHash`.

## Deploy and verify

First identify the exact installed executable and its existing Scheduled
Task or startup method. Confirm the target before replacing files. A shared
distribution directory does not establish the installed target.

1. Record the installed version, executable path, task name, and running state.
2. Use the tray menu `Exit` to stop CalendarSync before replacing its files.
   If the tray is unavailable, stop the recorded Scheduled Task. Confirm that
   the process has stopped.
3. Preserve the existing program files in an identified rollback directory.
   Keep this directory outside the replacement operation.
4. Copy only the tested package files into the installed directory. Preserve
   the existing `config.json`, `sync.log`, rotated logs, and runtime data.
   Do not copy deployed credentials into the package or release.
5. Compare the installed program-file hashes with the tested package hashes.
6. Restart the existing task if it was running. Use the recorded task name
   and task path. Preserve its account, triggers, privileges, and action.
7. Confirm the process path and build version. Check the next normal sync for
   the expected result. Keep calendar data and credentials out of reports.

If verification fails, stop CalendarSync. Restore the preserved program files
to the same installed directory. Keep the current configuration and logs.
Restart through the recorded startup method. Confirm the restored version and
process path. Retain the rollback files until deployment is accepted.

Remove agent-created release staging only after inspecting its exact path
and contents. Reject links and reparse points. Keep published assets and
deployment rollback files. Do not remove the Windows temporary root.
