![CalendarSync Illustrations](illustration.png "Calendar Sync")


# CalendarSync

**CalendarSync** is a lightweight .NET-based background application that performs a **one-way sync from Microsoft Outlook to Apple iCloud Calendar** using CalDAV. 
As a COM-Interop application it is optimized to run silently via a Scheduled Task on Windows systems - without relying on Microsoft Graph or full Exchange integration.

## Features

- One-Way syncs events from **local Outlook calendar** to **Apple iCloud calendar**
- Adds a **10-minute** and **3-minute alarm notification** for every synced event
- Runs silently and logs to `sync.log`
- Designed for **restricted corporate environments** — no UI required
- Tray icon with status tooltip

## Requirements

- Windows with Outlook installed and configured
- iCloud Calendar with CalDAV access
- .NET 8.0 or newer
- Basic access to Scheduled Tasks (admin rights only for registration)

## Quickstart

### 1. Build & Deploy

- Compile the app (`Release` mode).
- Copy the output (`.exe` + `config.json`) to a permanent path, e.g.:

```
C:\CalendarSync\
```

- Fill in `config.json` with your iCloud credentials and calendar info:

```json
{
    "ICloudCalDavUrl": "https://caldav.icloud.com",
    "ICloudUser": "your_apple_id@icloud.com",
    "ICloudPassword": "app-specific-password",
    "PrincipalId": "XXXXXXXXX",
    "WorkCalendarId": "YYYYYYYYY",
    "InitialWaitSeconds": 60,
    "SyncIntervalMinutes": 3,
    "SyncDaysIntoFuture": 30,
    "SyncDaysIntoPast": 30,
    "LogLevel": "Information"
}
```

Use a browser Dev Tools or CalDAV client to retrieve `PrincipalId` and `WorkCalendarId`.

### 2. Register as a Scheduled Task

Manual Method:

1. Open `Task Scheduler` → `Create Task`
2. General Tab:
   - Name: `CalendarSync`
   - Run only when user is logged on
   - Run with highest privileges
3. Triggers Tab: Add → `At log on`
4. Actions Tab:
   - Start a program → `C:\CalendarSync\CalendarSync.exe`

## Logs

Logs are written to:
```
C:\CalendarSync\sync.log
```

## Security

- Does not store or sync from iCloud to Outlook
- Passwords handled via basic auth over HTTPS

## Notes

- Outlook must be configured and ready on the host
- iCloud must be accessible via CalDAV (app-specific password required)

## License

MIT — use at your own risk and discretion.
