# CalendarSync

![CalendarSync](illustration.png "CalendarSync")

CalendarSync is a quiet .NET background application that performs a one-way
sync from local Microsoft Outlook calendars to Apple iCloud Calendar through
CalDAV. It uses Outlook COM interop and needs no Microsoft Graph or Exchange
integration.

## Behavior

- Sync one or more Outlook calendars into one iCloud destination.
- Identify each source and optionally prefix its events.
- Add 10-minute and 3-minute reminders to timed events. Do not add reminders
  to all-day or multi-day events.
- Run without a main window, show status in a tray icon, and write `sync.log`.
- Work as a per-user Windows Scheduled Task in restricted environments.

![Multiple calendar sources](illustration_multiple.png "Multiple calendar sources")

## Requirements

- Windows with Outlook configured for the interactive user
- iCloud CalDAV access and an app-specific password
- .NET 8 or newer
- Permission to register a Scheduled Task

## Build and configure

Build a Release package and copy it to a stable path such as
`C:\CalendarSync\`. Copy `config.example.json` to `config.json`, then set:

| Key | Purpose |
| --- | --- |
| `ICloudCalDavUrl` | HTTPS CalDAV endpoint. |
| `ICloudUser`, `ICloudPassword` | Apple ID and app-specific password. |
| `PrincipalId`, `WorkCalendarId` | CalDAV principal and destination calendar. |
| `InitialWaitSeconds`, `SyncIntervalMinutes` | Startup delay and sync cadence. |
| `SyncDaysIntoFuture`, `SyncDaysIntoPast` | Bounded synchronization window. |
| `LogLevel` | Log verbosity. |
| `SourceId` | Stable identifier for one Outlook source. |
| `EventTag` | Optional source prefix, such as `[COMPANY]`. |
| `SourceTimeZoneId`, `TargetTimeZoneId` | Optional source and destination zones. |
| `OutlookBodySyncMode` | `WhenSafe`, `Never`, or `Always`. |

Use browser developer tools or a CalDAV client to find the principal and
calendar IDs. If a time zone is absent, CalendarSync uses the host's local
zone. It validates HTTPS, identifiers, intervals, and windows before startup.
When `SourceId` is empty, it creates a stable ID and replaces the configuration
atomically.

## Scheduled Task

Create a task named `CalendarSync` that runs at user logon:

- Run only while the user is logged on.
- Run with highest privileges.
- Start `C:\CalendarSync\CalendarSync.exe`.

Run the program once as administrator if it must register the `CalendarSync`
source in the Windows Application event log. Detailed logs remain in
`C:\CalendarSync\sync.log`.

## Safety and COM reliability

CalendarSync never syncs from iCloud to Outlook. It sends basic authentication
only over HTTPS. It reads Outlook bodies only when Outlook and Windows report
that protected COM access is quiet. `Always` can trigger Outlook Object Model
Guard prompts.

Outlook work runs on one dedicated STA thread with timeouts. CalendarSync does
not start a second COM operation while a timed-out call is exiting. It makes no
iCloud changes when Outlook cannot produce a complete snapshot. Run the
[COM endurance procedure](docs/com-endurance.md) after a COM change.

## Validation

```powershell
.\scripts\Validate.ps1
```

The script performs locked restore, characterization tests, an x86 Release
build, dependency-layout checks, and a package self-test. It stages packages
under the Windows temporary directory. It does not read deployed `config.json`
or contact Outlook or iCloud.

License: MIT. Use at your own risk.
