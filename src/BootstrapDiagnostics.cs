using System.Runtime.InteropServices;

namespace CalendarSync;

internal static class BootstrapDiagnostics
{
	private static readonly string LogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bootstrap.log");
	private static readonly object Sync = new();
	private static bool _initialized;
	private static int _firstChanceCount;
	private const int MaxFirstChanceEntries = 5;
	private const int MaxAssemblyLoadEntries = 10;
	private static int _assemblyLoadCount;

	public static void Initialize()
	{
		if (_initialized)
			return;
        try
        {
            lock (Sync)
            {
                File.WriteAllText(LogPath, string.Empty);
            }
        }
        catch
        {
        }
        _initialized = true;
		Log($"Bootstrap diagnostics initialized. BaseDir={AppDomain.CurrentDomain.BaseDirectory}, WorkDir={Environment.CurrentDirectory}, Arch={RuntimeInformation.ProcessArchitecture}.");
		LogDirectorySnapshot(AppDomain.CurrentDomain.BaseDirectory, 50);
		AppDomain.CurrentDomain.ProcessExit += (_, _) => Log("Process exit observed.");
		AppDomain.CurrentDomain.FirstChanceException += (_, e) =>
		{
			var count = Interlocked.Increment(ref _firstChanceCount);
			if (count <= MaxFirstChanceEntries)
				Log($"First-chance exception: type={e.Exception.GetType().FullName}, HResult=0x{e.Exception.HResult:X8}.");
		};
		AppDomain.CurrentDomain.AssemblyLoad += (_, e) =>
		{
			var count = Interlocked.Increment(ref _assemblyLoadCount);
			if (count <= MaxAssemblyLoadEntries)
				Log($"Assembly loaded: {e.LoadedAssembly.FullName} ({e.LoadedAssembly.Location})");
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

	private static void LogDirectorySnapshot(string directory, int maxEntries)
	{
		try
		{
			if (!Directory.Exists(directory))
			{
				Log($"Directory snapshot skipped; missing directory {directory}.");
				return;
			}
			var entries = Directory.EnumerateFileSystemEntries(directory, "*", SearchOption.TopDirectoryOnly)
					.Take(maxEntries)
					.ToArray();
			Log($"Directory snapshot for {directory} ({entries.Length} entries, max {maxEntries}):");
			foreach (var entry in entries)
				Log($" - {entry}");
		}
		catch
		{
		}
	}
}
