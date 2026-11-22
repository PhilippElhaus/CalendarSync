using System.Net;
using System.Net.Http.Headers;
using System.Xml.Linq;
using Ical.Net;
using Ical.Net.Serialization;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CalendarSync.Tests;

public sealed class ReconciliationTests
{
	private const string BaseUrl = "https://caldav.example.test";
	private const string Collection = "/principal/calendars/calendar/";
	private const string Source = "fixture";
	private static readonly DateTime Start = new(2026, 8, 30, 10, 0, 0, DateTimeKind.Utc);
	private static readonly OutlookEventDto Desired = new("Synthetic subject", "Synthetic body", "Synthetic room",
		Start, Start.AddHours(1), Start, Start.AddHours(1), "synthetic-id", false, true);
	private static readonly string DesiredUid = Uid(Desired.GlobalId);
	private static readonly string StaleUid = Uid("stale-id");

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task IncompleteSnapshotMakesNoCalDavRequests(bool fullResync)
	{
		using var fake = new FakeServer();
		using var service = CreateService(fake, new CaptureLogger(), OutlookSnapshot.Incomplete());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, fullResync));
		Assert.Empty(fake.Requests);
	}

	[Fact]
	public async Task EquivalentRemoteEventDoesNotUploadAndLoggingFailureDoesNotChangeOutcome()
	{
		using var fake = new FakeServer(Listing(Member(DesiredUid)), Event(DesiredUid));
		using var service = CreateService(fake, new CaptureLogger(throwOnReport: true));
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "GET"], fake.Requests.Select(request => request.Method));
	}

	[Fact]
	public async Task ConditionalUpdatePreservesUnreadBodyAndUsesListedHref()
	{
		var prior = Desired with { Subject = "Previous synthetic subject", Body = "Preserved synthetic body" };
		var expected = Desired with { Body = prior.Body };
		using var fake = new FakeServer(Listing(Member(DesiredUid, extension: ".ICS")), Event(DesiredUid, prior),
			Status(HttpStatusCode.NoContent, "\"new\""), Event(DesiredUid, expected, "\"new\""));
		using var service = CreateService(fake, new CaptureLogger(), Snapshot(Desired with { Body = "", BodyWasRead = false }));
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "GET", "PUT", "GET"], fake.Requests.Select(request => request.Method));
		Assert.All(fake.Requests.Skip(1), request => Assert.EndsWith(".ICS", request.Path));
		Assert.Equal("\"listed\"", fake.Requests[2].IfMatch);
		Assert.Equal("\"new\"", fake.Requests[3].IfMatch);
		Assert.Equal(prior.Body, Calendar.Load(fake.Requests[2].Body!)!.Events.Single().Description);
	}

	[Theory]
	[InlineData("identity", "EventIdentityMismatch")]
	[InlineData("timing", "EventContentMismatch")]
	[InlineData("transient", "HttpRejected")]
	[InlineData("revision", "EventRevisionChanged")]
	public async Task VerificationFailureCannotCauseCorrectionOrDeletion(string mode, string reason)
	{
		var responses = new List<HttpResponseMessage> { Listing(Member(StaleUid)), Status(HttpStatusCode.Created, "\"new\"") };
		responses.Add(mode switch
		{
			"identity" => Event(Uid("foreign"), tag: "\"new\""),
			"timing" => Event(DesiredUid, Desired with { EndUtc = Start.AddHours(2) }, "\"new\""),
			"revision" => Event(DesiredUid),
			_ => Status(HttpStatusCode.ServiceUnavailable)
		});
		if (mode == "transient")
		{
			responses.Add(Status(HttpStatusCode.ServiceUnavailable));
		}
		using var fake = new FakeServer(responses.ToArray());
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger);
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, false));
		Assert.Equal("*", Assert.Single(fake.Requests, request => request.Method == "PUT").IfNoneMatch);
		Assert.DoesNotContain(fake.Requests, request => request.Method == "DELETE");
		var summary = Assert.Single(logger.Messages, message => message.StartsWith("SyncCycle ", StringComparison.Ordinal));
		Assert.Contains("Stage=Verify", summary);
		Assert.Contains($"Reason={reason}", summary);
		Assert.Contains("Written=1 Verified=0", summary);
		Assert.DoesNotContain(Desired.Subject, summary);
		Assert.DoesNotContain(DesiredUid, summary);
	}

	[Fact]
	public async Task VerificationAccessRetryDoesNotRepeatThePut()
	{
		using var fake = new FakeServer(Listing(), Status(HttpStatusCode.Created),
			Status(HttpStatusCode.Forbidden), Event(DesiredUid));
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "PUT", "GET", "GET"], fake.Requests.Select(request => request.Method));
	}

	[Fact]
	public async Task VerifiedWritePrecedesOwnershipReadAndConditionalDelete()
	{
		using var fake = new FakeServer(Listing(Member(StaleUid)), Status(HttpStatusCode.Created),
			Event(DesiredUid), Event(StaleUid), Status(HttpStatusCode.NoContent));
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger);
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "PUT", "GET", "GET", "DELETE"], fake.Requests.Select(request => request.Method));
		Assert.Equal("\"listed\"", fake.Requests[^1].IfMatch);
		Assert.Contains(logger.Messages, message => message.Contains("Written=1 Verified=1 StaleCandidates=1 OwnershipSkipped=0 Deleted=1", StringComparison.Ordinal));
		Assert.All(logger.Messages, message =>
		{
			Assert.DoesNotContain(Desired.Subject, message);
			Assert.DoesNotContain(DesiredUid, message);
			Assert.DoesNotContain(BaseUrl, message);
		});
	}

	[Fact]
	public async Task ManualWipeIncludesOtherSourceButStopsAtARevisionConflict()
	{
		var otherUid = CalendarRules.BuildManagedUid("other", "other-id", Start);
		using var fake = new FakeServer(Listing(Member(otherUid)), Status(HttpStatusCode.PreconditionFailed));
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, true));
		Assert.Equal(["PROPFIND", "DELETE"], fake.Requests.Select(request => request.Method));
		Assert.EndsWith(otherUid + ".ics", fake.Requests[1].Path);
		Assert.Equal("\"listed\"", fake.Requests[1].IfMatch);
	}

	[Theory]
	[InlineData(HttpStatusCode.PreconditionFailed)]
	[InlineData(HttpStatusCode.ServiceUnavailable)]
	[InlineData(HttpStatusCode.Accepted)]
	public async Task UnconfirmedMutationIsNotRepeatedOrFollowedByCleanup(HttpStatusCode status)
	{
		using var fake = new FakeServer(Listing(Member(StaleUid)), Status(status));
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "PUT"], fake.Requests.Select(request => request.Method));
	}

	[Theory]
	[InlineData("continuation")]
	[InlineData("missing-collection")]
	[InlineData("duplicate")]
	[InlineData("weak")]
	[InlineData("other-collection")]
	[InlineData("partial")]
	public async Task IncompleteOrAmbiguousListingStopsBeforeMutation(string mode)
	{
		using var listing = mode switch
		{
			"missing-collection" => Status(HttpStatusCode.MultiStatus),
			"duplicate" => Listing(Member(DesiredUid), Member(DesiredUid.ToUpperInvariant())),
			"weak" => Listing(Member(DesiredUid, tag: "W/\"listed\"")),
			"other-collection" => Listing(Member(DesiredUid, collection: "/other/")),
			_ => Listing(Member(StaleUid))
		};
		if (mode == "continuation")
		{
			listing.Headers.TryAddWithoutValidation("Link", "</next>; rel=next");
		}
		if (mode == "partial")
		{
			listing.StatusCode = HttpStatusCode.PartialContent;
		}
		using var fake = new FakeServer(listing);
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, false));
		Assert.Equal("PROPFIND", Assert.Single(fake.Requests).Method);
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public async Task CleanupRequiresOwnedContentAndTheListedRevision(bool owned)
	{
		var responses = new List<HttpResponseMessage>
		{
			Listing(Member(StaleUid), Member(CalendarRules.BuildManagedUid("other", "other-id", Start))),
			Event(owned ? StaleUid : Uid("foreign"))
		};
		if (owned)
		{
			responses.Add(Status(HttpStatusCode.PreconditionFailed));
		}
		using var fake = new FakeServer(responses.ToArray());
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger, Snapshot(null));
		Assert.Equal(owned ? CalendarSyncService.SyncCycleOutcome.Failed : CalendarSyncService.SyncCycleOutcome.Succeeded,
			await service.PerformSyncAsync(default, false));
		Assert.Equal(owned ? 3 : 2, fake.Requests.Count);
		Assert.Equal("\"listed\"", fake.Requests[1].IfMatch);
		if (owned)
		{
			Assert.Equal("DELETE", fake.Requests[2].Method);
			Assert.Equal("\"listed\"", fake.Requests[2].IfMatch);
		}
		else
		{
			Assert.Contains(logger.Messages, message => message.Contains("OwnershipSkipped=1 Deleted=0", StringComparison.Ordinal));
		}
	}

	private static CalendarSyncService CreateService(FakeServer server, CaptureLogger logger, OutlookSnapshot? snapshot = null) =>
		new(new SyncConfig { ICloudCalDavUrl = BaseUrl, PrincipalId = "principal", WorkCalendarId = "calendar", SourceId = Source },
			logger, _ => Task.FromResult(snapshot ?? Snapshot(Desired)), () => new HttpClient(server, false));

	private static OutlookSnapshot Snapshot(OutlookEventDto? dto) => new(
		dto == null ? [] : new Dictionary<string, OutlookEventDto> { [DesiredUid] = dto }, true, 0, false);

	private static string Uid(string id) => CalendarRules.BuildManagedUid(Source, id, Start);

	private static readonly XNamespace Dav = "DAV:";
	private static XElement PropertyResponse(string path, XElement prop) => new(Dav + "response",
		new XElement(Dav + "href", path), new XElement(Dav + "propstat", new XElement(Dav + "prop", prop),
			new XElement(Dav + "status", "HTTP/1.1 200 OK")));

	private static XElement Member(string uid, string extension = ".ics", string collection = Collection, string tag = "\"listed\"") =>
		PropertyResponse(collection + uid + extension, new XElement(Dav + "getetag", tag));

	private static HttpResponseMessage Listing(params XElement[] members) => new(HttpStatusCode.MultiStatus)
	{
		Content = new StringContent(new XElement(Dav + "multistatus",
			PropertyResponse(Collection, new XElement(Dav + "resourcetype", new XElement(Dav + "collection"))), members).ToString())
	};

	private static HttpResponseMessage Event(string uid, OutlookEventDto? dto = null, string tag = "\"listed\"")
	{
		var response = Status(HttpStatusCode.OK, tag);
		var calendar = new Calendar { Events = { CalendarRules.CreateCalendarEvent(dto ?? Desired, uid, null) } };
		response.Content = new StringContent(new CalendarSerializer().SerializeToString(calendar)!);
		return response;
	}

	private static HttpResponseMessage Status(HttpStatusCode status, string? tag = null)
	{
		var response = new HttpResponseMessage(status);
		response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.Zero);
		response.Headers.ETag = tag == null ? null : EntityTagHeaderValue.Parse(tag);
		return response;
	}

	private sealed record Request(string Method, string Path, string IfMatch, string IfNoneMatch, string? Body);

	private sealed class FakeServer(params HttpResponseMessage[] responses) : HttpMessageHandler
	{
		private readonly Queue<HttpResponseMessage> _responses = new(responses);
		internal List<Request> Requests { get; } = [];

		protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
		{
			Requests.Add(new Request(request.Method.Method, request.RequestUri!.AbsolutePath,
				request.Headers.IfMatch.ToString(), request.Headers.IfNoneMatch.ToString(),
				request.Method == HttpMethod.Put ? await request.Content!.ReadAsStringAsync(token) : null));
			return _responses.Dequeue();
		}

		protected override void Dispose(bool disposing)
		{
			while (_responses.TryDequeue(out var response))
			{
				response.Dispose();
			}
			base.Dispose(disposing);
		}
	}

	private sealed class CaptureLogger(bool throwOnReport = false) : ILogger<CalendarSyncService>
	{
		internal List<string> Messages { get; } = [];
		public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
		public bool IsEnabled(LogLevel level) => true;
		public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? exception,
			Func<TState, Exception?, string> formatter)
		{
			if (throwOnReport)
			{
				throw new IOException("Synthetic reporter failure");
			}
			Assert.Null(exception);
			Messages.Add(formatter(state, exception));
		}
	}
}
