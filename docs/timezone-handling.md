# Calendar time handling

- CalendarSync calculates one source-local day window for each scan. It includes
  today and the configured calendar days on each side. The end is exclusive.
  The Outlook date filter uses a wider interval to allow for differences between
  Outlook display time and source time. The final selection uses the source window.
- For timed events, Outlook UTC is authoritative. CalendarSync converts it to
  the configured source time zone for the window check. If Outlook UTC is
  absent, CalendarSync converts the Outlook wall-clock time in the source zone.
  Timed CalDAV data uses UTC. `TargetTimeZoneId` does not change the payload.
- An explicitly all-day event keeps its Outlook local dates. The exclusive end
  date does not shift when the source or target time zone is different. CalendarSync
  keeps the existing near-midnight heuristic for events not flagged as all-day.
- Recurrence expansion uses source-local wall-clock time. When Outlook does not
  supply the occurrence UTC, an ambiguous local time uses the system default
  offset. A missing or invalid time, including a time in a DST gap, can make the
  snapshot incomplete. An incomplete snapshot does not change iCloud.
- CalDAV verification compares timed values as absolute instants to the second.
  It compares all-day values as dates. Floating times cannot verify a timed copy.

Use the [COM endurance procedure](com-endurance.md) before releasing changes
that affect Outlook reads. Offline tests do not establish how an installed
Outlook provider exposes recurring masters or exceptions.
