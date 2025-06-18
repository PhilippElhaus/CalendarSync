using System.Diagnostics;

namespace CalendarSync.src;

public static class EventRecorder
{
    public const string Source = "CalendarSync";
    private const string LogName = "CalendarSync/Operations";
    private static bool _enabled;

    public static void Initialize()
    {
        try
        {
            if (!EventLog.SourceExists(Source))
                EventLog.CreateEventSource(Source, LogName);
            _enabled = true;
        }
        catch (Exception ex)
        {
            _enabled = false;
            Console.WriteLine($"Event log source setup failed: {ex.Message}. Run once as administrator to register.");
        }
    }

    public static void WriteEntry(string message, EventLogEntryType type)
    {
        if (!_enabled)
            return;
        try
        {
            EventLog.WriteEntry(Source, message, type);
        }
        catch (Exception ex)
        {
            _enabled = false;
            Console.WriteLine($"Event log write failed: {ex.Message}");
        }
    }
}
