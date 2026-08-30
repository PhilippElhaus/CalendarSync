# Outlook COM endurance check

Use this check only on the attended Windows workstation that owns the Outlook
profile. The check observes process metrics. It does not start CalendarSync,
trigger a sync, close Outlook, or change calendar data.

1. Start CalendarSync through its normal scheduled-task path.
2. Confirm that Outlook is configured and responsive.
3. Let CalendarSync complete at least one normal sync.
4. Run the following command from the repository:

   ```powershell
   .\scripts\Measure-OutlookComEndurance.ps1 `
     -DurationMinutes 30 `
     -AcknowledgeLiveOutlook
   ```

5. Confirm that the command reports a stable handle, thread, and private-byte
   plateau.
6. Review `sync.log` for overlapping-COM warnings, timeouts, or cleanup errors.

Also run these manual scenarios after a COM-related source change:

- Start CalendarSync while Outlook is already open.
- Start CalendarSync while Outlook is closed.
- Request one full re-sync during an active normal sync.
- Stop CalendarSync during the Outlook read.
- Confirm that Outlook remains responsive after the test.

Do not automate these scenarios against a production calendar. Use a dedicated
test calendar when the scenario can delete or replace target events.
