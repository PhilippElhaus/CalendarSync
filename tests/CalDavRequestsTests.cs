using System.Net;
using System.Net.Http.Headers;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace CalendarSync.Tests;

public sealed class CalDavRequestsTests
{
	[Theory]
	[InlineData(HttpStatusCode.Unauthorized)]
	[InlineData(HttpStatusCode.Forbidden)]
	public async Task VerificationRecoversFromOneAccessRejection(HttpStatusCode rejectedStatus)
	{
		var rejectedContent = new TrackingContent();
		using var handler = new SequenceHandler(
			Rejected(rejectedStatus, rejectedContent),
			new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("verified") });
		using var client = new HttpClient(handler);

		using var response = await SendAsync(client, HttpMethod.Get, retryVerificationAccess: true);

		Assert.Equal("verified", await response.Content.ReadAsStringAsync());
		Assert.Equal(2, handler.Requests.Count);
		Assert.NotSame(handler.Requests[0], handler.Requests[1]);
		Assert.True(rejectedContent.Disposed);
	}

	[Theory]
	[InlineData(HttpStatusCode.Unauthorized)]
	[InlineData(HttpStatusCode.Forbidden)]
	public async Task PersistentVerificationRejectionPreservesStatusAndOperation(HttpStatusCode status)
	{
		var firstContent = new TrackingContent();
		var secondContent = new TrackingContent();
		using var handler = new SequenceHandler(Rejected(status, firstContent), Rejected(status, secondContent));
		using var client = new HttpClient(handler);

		var error = await Assert.ThrowsAsync<CalDavAccessException>(
			() => SendAsync(client, HttpMethod.Get, retryVerificationAccess: true));

		Assert.Equal(status, error.StatusCode);
		Assert.Equal("event verification read", error.Operation);
		Assert.Contains($"HTTP {(int)status}", error.Message);
		Assert.DoesNotContain("caldav.example.test", error.Message);
		Assert.Equal(2, handler.Requests.Count);
		Assert.True(firstContent.Disposed);
		Assert.True(secondContent.Disposed);
	}

	[Theory]
	[InlineData("PUT")]
	[InlineData("DELETE")]
	[InlineData("PROPFIND")]
	public async Task ForbiddenWritesAndListingsAreNotRetried(string method)
	{
		using var handler = new SequenceHandler(Rejected(HttpStatusCode.Forbidden, new TrackingContent()));
		using var client = new HttpClient(handler);

		var error = await Assert.ThrowsAsync<CalDavAccessException>(
			() => SendAsync(client, new HttpMethod(method), retryVerificationAccess: true));

		Assert.Equal(HttpStatusCode.Forbidden, error.StatusCode);
		Assert.Single(handler.Requests);
	}

	[Fact]
	public async Task ForbiddenOrdinaryReadIsNotRetried()
	{
		using var handler = new SequenceHandler(Rejected(HttpStatusCode.Forbidden, new TrackingContent()));
		using var client = new HttpClient(handler);

		await Assert.ThrowsAsync<CalDavAccessException>(() => SendAsync(client, HttpMethod.Get));

		Assert.Single(handler.Requests);
	}

	[Fact]
	public async Task RetriedPutRecreatesItsBody()
	{
		using var handler = new SequenceHandler(
			Rejected(HttpStatusCode.Unauthorized, new TrackingContent()),
			new HttpResponseMessage(HttpStatusCode.Created));
		using var client = new HttpClient(handler);

		using var response = await SendAsync(client, HttpMethod.Put);

		Assert.Equal(HttpStatusCode.Created, response.StatusCode);
		Assert.Equal(new[] { "calendar data", "calendar data" }, handler.Bodies);
		Assert.NotSame(handler.Requests[0].Content, handler.Requests[1].Content);
	}

	[Fact]
	public async Task CancellationDuringRetryDelayStopsFurtherRequests()
	{
		using var cancellation = new CancellationTokenSource();
		var content = new TrackingContent(cancellation.Cancel);
		var rejection = Rejected(HttpStatusCode.Unauthorized, content);
		rejection.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(30));
		using var handler = new SequenceHandler(rejection);
		using var client = new HttpClient(handler);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => SendAsync(client, HttpMethod.Get, token: cancellation.Token));

		Assert.Single(handler.Requests);
		Assert.True(content.Disposed);
	}

	[Fact]
	public async Task LocalAccessExceptionIsNotReportedAsCalDavAuthentication()
	{
		using var handler = new ThrowingHandler();
		using var client = new HttpClient(handler);

		await Assert.ThrowsAsync<UnauthorizedAccessException>(() => SendAsync(client, HttpMethod.Get));
	}

	[Fact]
	public async Task ServerErrorStillRetriesAndBadRequestDoesNot()
	{
		using var handler = new SequenceHandler(
			Rejected(HttpStatusCode.ServiceUnavailable, new TrackingContent()),
			new HttpResponseMessage(HttpStatusCode.BadRequest));
		using var client = new HttpClient(handler);

		var error = await Assert.ThrowsAsync<HttpRequestException>(() => SendAsync(client, HttpMethod.Get));

		Assert.Equal(HttpStatusCode.BadRequest, error.StatusCode);
		Assert.Equal(2, handler.Requests.Count);
	}

	private static Task<HttpResponseMessage> SendAsync(
		HttpClient client, HttpMethod method, bool retryVerificationAccess = false, CancellationToken token = default) =>
		CalDavRequests.SendAsync(client,
			() => new HttpRequestMessage(method, "https://caldav.example.test/calendar/event.ics")
			{
				Content = method == HttpMethod.Put ? new StringContent("calendar data") : null
			},
			"event verification read", token, NullLogger.Instance, retryVerificationAccess);

	private static HttpResponseMessage Rejected(HttpStatusCode status, HttpContent content)
	{
		var response = new HttpResponseMessage(status) { Content = content };
		response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.Zero);
		return response;
	}

	private sealed class SequenceHandler(params HttpResponseMessage[] responses) : HttpMessageHandler
	{
		private readonly Queue<HttpResponseMessage> _responses = new(responses);
		internal List<HttpRequestMessage> Requests { get; } = new();
		internal List<string> Bodies { get; } = new();

		protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
		{
			Requests.Add(request);
			if (request.Content != null)
			{
				Bodies.Add(await request.Content.ReadAsStringAsync(token));
			}

			return _responses.Dequeue();
		}
	}

	private sealed class ThrowingHandler : HttpMessageHandler
	{
		protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token) =>
			throw new UnauthorizedAccessException("Local access denied.");
	}

	private sealed class TrackingContent(Action? onDispose = null) : StringContent("request rejected")
	{
		internal bool Disposed { get; private set; }

		protected override void Dispose(bool disposing)
		{
			Disposed = true;
			onDispose?.Invoke();
			base.Dispose(disposing);
		}
	}
}
