using System.Reflection;
using System.Runtime.Loader;
using System.Threading;

namespace CalendarSync;

internal static class BootstrapDiagnostics
{
	private static readonly string LogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bootstrap.log");
	private static readonly object Sync = new();
	private static bool _initialized;
	private static int _firstChanceCount;
	private const int MaxFirstChanceEntries = 5;

	public static void Initialize()
	{
		if (_initialized)
			return;
		_initialized = true;
		Log("Bootstrap diagnostics initialized.");
		AppDomain.CurrentDomain.ProcessExit += (_, _) => Log("Process exit observed.");
		AppDomain.CurrentDomain.FirstChanceException += (_, e) =>
		{
			var count = Interlocked.Increment(ref _firstChanceCount);
			if (count <= MaxFirstChanceEntries)
				Log($"First-chance exception: {e.Exception}");
		};
	}

	public static void Log(string message)
	{
		try
		{
			var line = $"[{DateTime.UtcNow:u}] {message}{Environment.NewLine}";
			lock (Sync)
			{
				File.AppendAllText(LogPath, line);
			}
		}
		catch
		{
		}
	}

	public static void LogAssemblyProbe(string assemblyName, string candidatePath, bool exists, bool loaded)
	{
		Log($"Assembly probe for {assemblyName}: path={candidatePath}, exists={exists}, loaded={loaded}.");
	}

	public static void LogNativeProbe(string libraryName, string candidatePath, bool exists, bool loaded)
	{
		Log($"Native probe for {libraryName}: path={candidatePath}, exists={exists}, loaded={loaded}.");
	}
}
