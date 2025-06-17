using CalendarSync.src;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Serilog;
using Serilog.Events;
using System.Diagnostics;
using System.Reflection;

namespace CalendarSync;

public class Program
{
        [STAThread]
        public static void Main(string[] args)
        {
                EventRecorder.Initialize();
                SubscribeToGlobalExceptions();
                EventRecorder.WriteEntry("Application startup", EventLogEntryType.Information);

                using var host = CreateHostBuilder(args).Build();
                var tray = host.Services.GetRequiredService<TrayIconManager>();
		
                tray.ExitClicked += async (_, _) =>
                {
                        EventRecorder.WriteEntry("Shutdown requested", EventLogEntryType.Information);
                        await host.StopAsync();
                        tray.Dispose();
                        Application.Exit();
                };
		
                host.StartAsync().GetAwaiter().GetResult();
                Application.Run();
                EventRecorder.WriteEntry("Application shutdown", EventLogEntryType.Information);
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
                Host.CreateDefaultBuilder(args)
                        .ConfigureServices((hostContext, services) =>
                        {
				
                                var configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");
                                if (!File.Exists(configPath))
                                {
                                        EventRecorder.WriteEntry("config.json not found", EventLogEntryType.Error);
                                        throw new FileNotFoundException("config.json not found in the executable directory.");
                                }
				var configJson = File.ReadAllText(configPath);
				var config = JsonConvert.DeserializeObject<SyncConfig>(configJson);

				services.AddSingleton<SyncConfig>(config!);
				services.AddSingleton<TrayIconManager>();
				services.AddHostedService<CalendarSyncService>();

				LogEventLevel serilogLevel = LogEventLevel.Information;
				if (!string.IsNullOrWhiteSpace(config!.LogLevel) &&
					Enum.TryParse(config.LogLevel, true, out LogEventLevel parsedLevel))
				{
					serilogLevel = parsedLevel;
				}
				var logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sync.log");

				var logger = new LoggerConfiguration()
					.MinimumLevel.Is(serilogLevel)
						.WriteTo.File(
						logFilePath,
						rollOnFileSizeLimit: true,
						fileSizeLimitBytes: 1_048_576,
						rollingInterval: RollingInterval.Infinite,
						retainedFileCountLimit: 1, 
						shared: true
						)
					.CreateLogger();

                                services.AddLogging(builder => builder.AddSerilog(logger, dispose: true));
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
                EventRecorder.WriteEntry(ex.ToString(), EventLogEntryType.Error);
        }
}