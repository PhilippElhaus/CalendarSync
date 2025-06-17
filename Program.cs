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

		string source = "CalendarSync";
		string logName = "Application";

		if (!EventLog.SourceExists(source))
		{			
			EventLog.CreateEventSource(source, logName);
			Process.Start(new ProcessStartInfo
			{
				FileName = Assembly.GetExecutingAssembly().Location,
				UseShellExecute = true,
				Verb = "runas"
			});
			return;
		}

		using EventLog eventLog = new EventLog(logName)
		{
			Source = source
		};

		using var host = CreateHostBuilder(args).Build();
		var tray = host.Services.GetRequiredService<TrayIconManager>();
		
		tray.ExitClicked += async (_, _) =>
		{
		await host.StopAsync();
		tray.Dispose();
		Application.Exit();
		};
		
		host.StartAsync().GetAwaiter().GetResult();
		Application.Run();
	}

	public static IHostBuilder CreateHostBuilder(string[] args) =>
		Host.CreateDefaultBuilder(args)				
			.ConfigureServices((hostContext, services) =>
			{
				
				var configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");
				if (!File.Exists(configPath))
				{
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
				EventLog.WriteEntry("Main", "Read Config & Started Logging", EventLogEntryType.Information);
			});
}