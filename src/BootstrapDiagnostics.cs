using System.Runtime.InteropServices;
using System.Text;

namespace CalendarSync;

internal static class BootstrapDiagnostics
{
	private const int SizeLimit = 64 * 1024;
	private static readonly string LogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bootstrap.log");
	private static readonly object Sync = new();
	private static int _probeCount;

	public static void Initialize() => Log($"Bootstrap started. Architecture={RuntimeInformation.ProcessArchitecture}.");

	public static void Log(string message)
	{
		try
		{
			var line = Encoding.UTF8.GetBytes($"{DateTimeOffset.UtcNow:O} {message}{Environment.NewLine}");
			if (line.Length > SizeLimit)
			{
				return;
			}
			lock (Sync)
			{
				using var stream = new FileStream(LogPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
				if (stream.Length > SizeLimit - line.Length)
				{
					stream.SetLength(0);
				}
				stream.Position = stream.Length;
				stream.Write(line);
			}
		}
		catch { }
	}

	public static void LogProbe(string kind, bool exists, bool loaded)
	{
		if (Interlocked.Increment(ref _probeCount) <= 20)
		{
			Log($"{kind} dependency probe: exists={exists}, loaded={loaded}.");
		}
	}
}
