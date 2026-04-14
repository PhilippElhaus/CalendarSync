using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Serilog;
using Serilog.Events;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Reflection;
using System.Windows.Forms;

namespace CalendarSync;

public class Program
{
	private static readonly Lazy<string> BinDirectory = new(() =>
	{
		var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bin");
		if (!Directory.Exists(path))
			Directory.CreateDirectory(path);
		return path;
	});

	static Program()
	{
		BootstrapDiagnostics.Initialize();
		AppDomain.CurrentDomain.AssemblyResolve += ResolveFromBin;
		AssemblyLoadContext.Default.Resolving += ResolveFromBin;
		AssemblyLoadContext.Default.ResolvingUnmanagedDll += ResolveNativeFromBin;
	}

	[STAThread]
	public static void Main(string[] args)
	{
		Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);
		try
		{
			BootstrapDiagnostics.Log("Main entry reached.");
			EventRecorder.Initialize();
			using var singleInstanceMutex = new System.Threading.Mutex(true, "CalendarSync", out var createdNewInstance);
			if (!createdNewInstance)
			{
				BootstrapDiagnostics.Log("Another instance detected, exiting.");
				return;
			}
			SubscribeToGlobalExceptions();
			EventRecorder.WriteEntry("Application startup", EventLogEntryType.Information);

			using var host = CreateHostBuilder(args).Build();
			var tray = host.Services.GetRequiredService<TrayIconManager>();
			var service = host.Services.GetRequiredService<CalendarSyncService>();

			tray.ExitClicked += async (_, _) =>
			{
				EventRecorder.WriteEntry("Shutdown requested", EventLogEntryType.Information);
				await host.StopAsync().ConfigureAwait(false);
				tray.Dispose();
				Application.Exit();
			};

			tray.FullResyncClicked += async (_, _) =>
			{
				await service.TriggerFullResyncAsync().ConfigureAwait(false);
			};

			host.StartAsync().GetAwaiter().GetResult();
			BootstrapDiagnostics.Log("Host started, entering message loop.");
			Application.Run();
			EventRecorder.WriteEntry("Application shutdown", EventLogEntryType.Information);
		}
		catch (Exception ex)
		{
			BootstrapDiagnostics.Log($"Fatal exception in Main: {ex}");
			EventRecorder.WriteEntry($"Fatal startup error: {ex}", EventLogEntryType.Error);
			try
			{
				MessageBox.Show(
					"CalendarSync failed to start. Check bootstrap.log and sync.log in the application directory for details.",
					"CalendarSync Startup Error",
					MessageBoxButtons.OK,
					MessageBoxIcon.Error);
			}
			catch
			{
			}

			Environment.ExitCode = 1;
		}
	}

	public static IHostBuilder CreateHostBuilder(string[] args) =>
			Host.CreateDefaultBuilder(args)
					.ConfigureServices((hostContext, services) =>
					{
						var configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");
						if (!File.Exists(configPath))
						{
							BootstrapDiagnostics.Log($"config.json missing at {configPath}.");
							EventRecorder.WriteEntry("config.json not found", EventLogEntryType.Error);
							throw new FileNotFoundException("config.json not found in the executable directory.");
						}
						var configJson = File.ReadAllText(configPath);
						var config = JsonConvert.DeserializeObject<SyncConfig>(configJson);
						if (string.IsNullOrWhiteSpace(config?.SourceId))
						{
							config!.SourceId = Guid.NewGuid().ToString("N");
							File.WriteAllText(configPath, JsonConvert.SerializeObject(config, Formatting.Indented));
						}

						services.AddSingleton<SyncConfig>(config!);
						services.AddSingleton<TrayIconManager>();
						services.AddSingleton<CalendarSyncService>();
						services.AddSingleton<IHostedService>(sp =>
							sp.GetRequiredService<CalendarSyncService>());

						LogEventLevel serilogLevel = LogEventLevel.Information;
						if (!string.IsNullOrWhiteSpace(config!.LogLevel) &&
								Enum.TryParse(config.LogLevel, true, out LogEventLevel parsedLevel))
						{
							serilogLevel = parsedLevel;
						}
						var logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sync.log");
						var oldLogFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sync.log.old");

						var logger = new LoggerConfiguration()
								.MinimumLevel.Is(serilogLevel)
								.WriteTo.Sink(new TwoFileRollingSink(logFilePath, oldLogFilePath, 1_048_576))
								.CreateLogger();

						services.AddLogging(builder => builder.AddSerilog(logger, dispose: true));
						BootstrapDiagnostics.Log("Services configured successfully.");
						EventRecorder.WriteEntry("Configuration loaded", EventLogEntryType.Information);
					});

	private static void SubscribeToGlobalExceptions()
	{
		AppDomain.CurrentDomain.UnhandledException += (_, e) => HandleGlobalException(e.ExceptionObject as Exception);
		TaskScheduler.UnobservedTaskException += (_, e) =>
		{
			HandleGlobalException(e.Exception);
			e.SetObserved();
		};
		Application.ThreadException += (_, e) => HandleGlobalException(e.Exception);
	}

	private static void HandleGlobalException(Exception? ex)
	{
		if (ex == null)
			return;
		try
		{
			Log.Fatal(ex, "Unhandled exception");
		}
		catch { }
		BootstrapDiagnostics.Log($"Unhandled exception captured: {ex}");
		EventRecorder.WriteEntry(ex.ToString(), EventLogEntryType.Error);
	}

	private static Assembly? ResolveFromBin(object? _, ResolveEventArgs args)
	{
		return ResolveManagedFromBin(new AssemblyName(args.Name));
	}

	private static Assembly? ResolveFromBin(AssemblyLoadContext context, AssemblyName assemblyName)
	{
		var resolved = ResolveManagedFromBin(assemblyName);
		if (resolved != null)
			return resolved;
		return null;
	}

	private static Assembly? ResolveManagedFromBin(AssemblyName assemblyName)
	{
		var candidatePath = Path.Combine(BinDirectory.Value, $"{assemblyName.Name}.dll");
		if (!File.Exists(candidatePath))
		{
			BootstrapDiagnostics.LogAssemblyProbe(assemblyName.Name ?? "unknown", candidatePath, false, false);
			return null;
		}
		var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(candidatePath);
		BootstrapDiagnostics.LogAssemblyProbe(assemblyName.Name ?? "unknown", candidatePath, true, assembly != null);
		return assembly;
	}

	private static IntPtr ResolveNativeFromBin(Assembly _, string name)
	{
		var candidatePath = Path.Combine(BinDirectory.Value, $"{name}.dll");
		if (!File.Exists(candidatePath))
		{
			BootstrapDiagnostics.LogNativeProbe(name, candidatePath, false, false);
			return IntPtr.Zero;
		}
		var handle = NativeLibrary.Load(candidatePath);
		BootstrapDiagnostics.LogNativeProbe(name, candidatePath, true, handle != IntPtr.Zero);
		return handle;
	}
}
