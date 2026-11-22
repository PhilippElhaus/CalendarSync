using Serilog;
using Xunit;

namespace CalendarSync.Tests;

public sealed class TwoFileRollingSinkTests
{
	[Fact]
	public void OversizedUtf8EntryAndExceptionDetailsNeverReachRolledFiles()
	{
		var tempRoot = Path.GetFullPath(Path.GetTempPath());
		Assert.NotEqual("D:\\", Path.GetPathRoot(tempRoot));
		var directory = Path.Combine(tempRoot, $"CalendarSync.LogTests.{Guid.NewGuid():N}");
		Directory.CreateDirectory(directory);
		try
		{
			var current = Path.Combine(directory, "sync.log");
			var previous = Path.Combine(directory, "sync.log.old");
			const int cap = 160;
			using (var logger = new LoggerConfiguration().WriteTo.Sink(new TwoFileRollingSink(current, previous, cap)).CreateLogger())
			{
				logger.Information("{Entry}", new string('\u20AC', 100));
				logger.Error(new IOException("Synthetic exception contents"), "Synthetic safe error");
				logger.Information("{Entry}", new string('\u20AC', 100));
				logger.Information("End of synthetic log");
			}
			Assert.True(File.Exists(previous));
			foreach (var path in new[] { current, previous })
			{
				Assert.InRange(new FileInfo(path).Length, 1, cap);
				Assert.DoesNotContain("Synthetic exception contents", File.ReadAllText(path));
				Assert.DoesNotContain(new string('\u20AC', 100), File.ReadAllText(path));
			}
		}
		finally
		{
			foreach (var name in new[] { "sync.log", "sync.log.old" })
			{
				File.Delete(Path.Combine(directory, name));
			}
			Directory.Delete(directory);
		}
	}

	[Fact]
	public void SinkKeepsOnlyCurrentAndPreviousLogFiles()
	{
		var tempRoot = Path.GetFullPath(Path.GetTempPath());
		var directory = Path.Combine(tempRoot, $"CalendarSync.LogTests.{Guid.NewGuid():N}");
		Directory.CreateDirectory(directory);
		var current = Path.Combine(directory, "sync.log");
		var previous = Path.Combine(directory, "sync.log.old");

		try
		{
			using var sink = new TwoFileRollingSink(current, previous, 160);
			using var logger = new LoggerConfiguration().WriteTo.Sink(sink).CreateLogger();
			for (var index = 0; index < 10; index++)
			{
				logger.Information("Rolling log characterization entry {Index} with fixed payload", index);
			}

			Assert.True(File.Exists(current));
			Assert.True(File.Exists(previous));
			Assert.Equal(2, Directory.GetFiles(directory, "sync.log*").Length);
		}
		finally
		{
			var resolved = Path.GetFullPath(directory);
			if (resolved.Contains("CalendarSync.LogTests.", StringComparison.Ordinal) && Directory.Exists(resolved))
			{
				Directory.Delete(resolved, true);
			}
		}
	}
}
