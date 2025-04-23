using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Serilog;

namespace CalendarSync
{
	public class Program
	{
		public static void Main(string[] args)
		{
			CreateHostBuilder(args).Build().Run();
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
					services.AddHostedService<CalendarSyncService>();
					services.AddLogging(builder =>
					{
						builder.AddSerilog(new LoggerConfiguration()
							.MinimumLevel.Debug()
							.WriteTo.File(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sync.log"))
							.CreateLogger(), dispose: true);
					});
				});
	}

	public class SyncConfig
	{
		public string ICloudCalDavUrl { get; set; }
		public string ICloudUser { get; set; }
		public string ICloudPassword { get; set; }
		public string PrincipalId { get; set; }
		public string WorkCalendarId { get; set; }
		public int InitialWaitSeconds { get; set; } = 60;
		public int SyncIntervalMinutes { get; set; } = 3;
		public int SyncDaysIntoFuture { get; set; } = 30;

	}
}