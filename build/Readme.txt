CalendarSync Build QuickStart
1) Build in Release mode: dotnet build -c Release
2) Copy CalendarSync.exe, config.json, and the bin subfolder from the build output into your install directory (e.g., C:\CalendarSync\).
3) Edit config.json with your iCloud URL, Apple ID, app password, PrincipalId, and WorkCalendarId.
4) Run CalendarSync.exe once to verify log output (sync.log) and then register it as a Scheduled Task (At log on → run with highest privileges).
