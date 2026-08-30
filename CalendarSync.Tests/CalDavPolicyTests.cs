using System.Net;
using Xunit;

namespace CalendarSync.Tests;

public sealed class CalDavPolicyTests
{
	[Theory]
	[InlineData(HttpStatusCode.RequestTimeout, true)]
	[InlineData((HttpStatusCode)429, true)]
	[InlineData(HttpStatusCode.InternalServerError, true)]
	[InlineData(HttpStatusCode.BadRequest, false)]
	[InlineData(HttpStatusCode.NotFound, false)]
	public void RetryPolicyOnlyRetriesTransientStatuses(HttpStatusCode statusCode, bool expected)
	{
		Assert.Equal(expected, CalendarSyncService.IsTransientCalDavStatus(statusCode));
	}
}
