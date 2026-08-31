using Newtonsoft.Json;
using Xunit;

namespace CalendarSync.Tests;

public sealed class SyncConfigLoaderTests
{
	[Fact]
	public void LoadCreatesAndPersistsASafeSourceId()
	{
		using var temp = new TemporaryDirectory();
		var path = Path.Combine(temp.Path, "config.json");
		File.WriteAllText(path, JsonConvert.SerializeObject(CreateValidConfig()));

		var loaded = SyncConfigLoader.Load(path);
		var reloaded = JsonConvert.DeserializeObject<SyncConfig>(File.ReadAllText(path));

		Assert.Matches("^[a-f0-9]{32}$", loaded.SourceId!);
		Assert.Equal(loaded.SourceId, reloaded?.SourceId);
	}

	[Theory]
	[InlineData("http://caldav.example.test")]
	[InlineData("not-a-url")]
	public void ValidateRejectsNonHttpsCalDavEndpoints(string endpoint)
	{
		var config = CreateValidConfig();
		config.ICloudCalDavUrl = endpoint;

		Assert.Throws<InvalidDataException>(() => SyncConfigLoader.Validate(config));
	}

	[Fact]
	public void ValidateRejectsUnsafeSourceIdsAndIntervals()
	{
		var unsafeSource = CreateValidConfig();
		unsafeSource.SourceId = "source/other";
		Assert.Throws<InvalidDataException>(() => SyncConfigLoader.Validate(unsafeSource));

		var invalidInterval = CreateValidConfig();
		invalidInterval.SyncIntervalMinutes = 0;
		Assert.Throws<InvalidDataException>(() => SyncConfigLoader.Validate(invalidInterval));
	}

	[Fact]
	public void DistributedConfigurationExamplePassesValidation()
	{
		var examplePath = Path.GetFullPath(Path.Combine(
			AppContext.BaseDirectory,
			"..",
			"..",
			"..",
			"..",
			"build",
			"config.example.json"));
		var example = JsonConvert.DeserializeObject<SyncConfig>(File.ReadAllText(examplePath));

		Assert.NotNull(example);
		SyncConfigLoader.Validate(example);
	}

	private static SyncConfig CreateValidConfig() => new()
	{
		ICloudCalDavUrl = "https://caldav.example.test",
		ICloudUser = "test@example.test",
		ICloudPassword = "not-a-real-password",
		PrincipalId = "123456",
		WorkCalendarId = "calendar-id",
		InitialWaitSeconds = 0,
		SyncIntervalMinutes = 3,
		SyncDaysIntoFuture = 30,
		SyncDaysIntoPast = 30,
		LogLevel = "Information",
		SourceId = string.Empty,
		OutlookBodySyncMode = "WhenSafe"
	};

	private sealed class TemporaryDirectory : IDisposable
	{
		public TemporaryDirectory()
		{
			var tempRoot = System.IO.Path.GetFullPath(System.IO.Path.GetTempPath());
			if (System.IO.Path.GetPathRoot(tempRoot)?.Equals("D:\\", StringComparison.OrdinalIgnoreCase) == true)
			{
				throw new InvalidOperationException("Tests cannot use D: as a temporary root.");
			}

			Path = System.IO.Path.Combine(tempRoot, $"CalendarSync.Tests.{Guid.NewGuid():N}");
			Directory.CreateDirectory(Path);
		}

		public string Path { get; }

		public void Dispose()
		{
			var resolved = System.IO.Path.GetFullPath(Path);
			if (resolved.Contains("CalendarSync.Tests.", StringComparison.Ordinal) && Directory.Exists(resolved))
			{
				Directory.Delete(resolved, true);
			}
		}
	}
}
