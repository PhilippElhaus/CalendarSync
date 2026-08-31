using Serilog;
using Xunit;

namespace CalendarSync.Tests;

public sealed class TwoFileRollingSinkTests
{
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
