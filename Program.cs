using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Serilog;
using Serilog.Events;
using System;

namespace CalendarSync
{
	public class Program
	{
		[STAThread]
		public static void Main(string[] args)
		{
			using var host = CreateHostBuilder(args).Build();
			var tray = host.Services.GetRequiredService<TrayIconManager>();
			host.Run();
			tray.Dispose();
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


					services.AddSingleton(config);
					services.AddSingleton<TrayIconManager>();
					services.AddHostedService<CalendarSyncService>();

					LogEventLevel serilogLevel = LogEventLevel.Information;
					if (!string.IsNullOrWhiteSpace(config.LogLevel) &&
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
				});
	}

	public class SyncConfig
	{
		public string ICloudCalDavUrl { get; set; }
		public string ICloudUser { get; set; }
		public string ICloudPassword { get; set; }
		public string PrincipalId { get; set; }
		public string WorkCalendarId { get; set; }
		public string LogLevel { get; set; } = "Information"; 
		public int InitialWaitSeconds { get; set; } = 60;
		public int SyncIntervalMinutes { get; set; } = 3;
		public int SyncDaysIntoFuture { get; set; } = 30;
		public int SyncDaysIntoPast { get; set; } = 30;

	}
}