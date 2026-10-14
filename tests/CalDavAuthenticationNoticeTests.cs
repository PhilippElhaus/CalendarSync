using System.Net;
using Xunit;

namespace CalendarSync.Tests;

public sealed class CalDavAuthenticationNoticeTests
{
	private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

	[Fact]
	public void FirstAuthenticationFailureDoesNotShowANotice()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
	}

	[Fact]
	public void SecondConsecutiveAuthenticationFailureShowsANotice()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(1)));
	}

	[Theory]
	[InlineData(null)]
	[InlineData(HttpStatusCode.Forbidden)]
	[InlineData(HttpStatusCode.BadRequest)]
	[InlineData(HttpStatusCode.InternalServerError)]
	public void OtherOutcomesResetConsecutiveAuthenticationFailures(HttpStatusCode? status)
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.False(notice.RecordCycle(status, false, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(2)));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(3)));
	}

	[Fact]
	public void SuccessfulRequestInAFailedCycleResetsConsecutiveAuthenticationFailures()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, true, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(2)));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(3)));
	}

	[Fact]
	public void RepeatedResourceFailuresAfterSuccessfulRequestsDoNotShowCredentialNotices()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, true, Start));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, true, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(2)));
	}

	[Fact]
	public void NoticesHaveAThirtyMinuteCooldown()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(30)));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(31)));
	}

	[Theory]
	[InlineData(null, true)]
	[InlineData(HttpStatusCode.Forbidden, false)]
	[InlineData(HttpStatusCode.Unauthorized, true)]
	public void ResetPreservesTheNoticeCooldown(HttpStatusCode? status, bool hadSuccessfulRequest)
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(status, hadSuccessfulRequest, Start.AddMinutes(2)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(3)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(4)));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(31)));
	}

	[Fact]
	public void AResetRequiresTwoNewFailuresEvenAfterTheCooldownEnds()
	{
		var notice = new CalDavAuthenticationNotice();

		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(1)));
		Assert.False(notice.RecordCycle(null, true, Start.AddMinutes(30)));
		Assert.False(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(31)));
		Assert.True(notice.RecordCycle(HttpStatusCode.Unauthorized, false, Start.AddMinutes(32)));
	}
}
