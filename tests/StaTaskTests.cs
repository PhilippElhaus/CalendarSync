using Xunit;

namespace CalendarSync.Tests;

public sealed class StaTaskTests
{
	[Fact]
	public async Task PreCanceledWorkerReleasesOwnershipWithoutEnteringCom()
	{
		using var source = new CancellationTokenSource();
		source.Cancel();
		var ownershipReleased = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var entered = false;
		var task = StaTask.Run(() =>
		{
			entered = true;
			return 0;
		}, source.Token, () => ownershipReleased.SetResult());
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
		await ownershipReleased.Task.WaitAsync(TimeSpan.FromSeconds(5));
		Assert.False(entered);
	}
}
