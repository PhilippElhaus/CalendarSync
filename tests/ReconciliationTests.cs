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
		Assert.True(fake.Requests[3].NoCache);
		Assert.Equal(prior.Body, Calendar.Load(fake.Requests[2].Body!)!.Events.Single().Description);
	}

	[Theory]
	[InlineData("identity", "EventIdentityMismatch")]
	[InlineData("timing", "EventContentMismatch")]
	[InlineData("transient", "HttpRejected")]
	[InlineData("revision", "EventRevisionChanged")]
	[InlineData("timezone", "EventTimeUnresolvable")]
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
		if (mode == "timezone")
		{
			responses[2].Dispose();
			responses[2] = Event(DesiredUid, tag: "\"new\"");
			await ChangeEventText(responses[2], text => text.Replace("DTSTART:20260830T100000Z",
				"DTSTART;TZID=Synthetic/Unknown:20260830T100000", StringComparison.Ordinal));
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
		Assert.Contains("MutationOutcome=Partial WritePossible=0", summary);
		Assert.DoesNotContain(Desired.Subject, summary);
		Assert.DoesNotContain(DesiredUid, summary);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("W/\"normalized\"")]
	public async Task VerificationAccessRetryDoesNotRepeatThePut(string? putTag)
	{
		using var fake = new FakeServer(Listing(), Status(HttpStatusCode.Created, putTag),
			Status(HttpStatusCode.Forbidden), Event(DesiredUid));
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "PUT", "GET", "GET"], fake.Requests.Select(request => request.Method));
		Assert.All(fake.Requests.Skip(2), request =>
		{
			Assert.Empty(request.IfMatch);
			Assert.True(request.NoCache);
		});
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
		Assert.Contains("MutationOutcome=Confirmed", Summary(logger));
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
	[InlineData(HttpStatusCode.PreconditionFailed, "None", 0)]
	[InlineData(HttpStatusCode.ServiceUnavailable, "Possible", 1)]
	[InlineData(HttpStatusCode.RequestTimeout, "Possible", 1)]
	[InlineData(HttpStatusCode.Accepted, "Possible", 1)]
	[InlineData(HttpStatusCode.MultiStatus, "Possible", 1)]
	public async Task UnconfirmedMutationIsNotRepeatedOrFollowedByCleanup(HttpStatusCode status, string outcome, int possible)
	{
		using var fake = new FakeServer(Listing(Member(StaleUid)), Status(status));
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger);
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "PUT"], fake.Requests.Select(request => request.Method));
		var summary = Summary(logger);
		Assert.Contains($"HttpStatus={(int)status} ", summary);
		Assert.Contains("Written=0 Verified=0", summary);
		Assert.Contains($"MutationOutcome={outcome} WritePossible={possible}", summary);
	}

	[Theory]
	[InlineData("transport", false, "Possible", 1)]
	[InlineData("timeout", false, "Possible", 1)]
	[InlineData("cancel-in-flight", true, "Possible", 1)]
	[InlineData("cancel-after-401", true, "None", 0)]
	public async Task InterruptedPutCountsOnlyPotentiallyAppliedRequests(string mode, bool canceled,
		string mutationOutcome, int possible)
	{
		using var cancellation = new CancellationTokenSource();
		using var fake = new FakeServer(Listing(Member(StaleUid)))
		{
			Override = request =>
			{
				if (request.Method != HttpMethod.Put)
				{
					return null;
				}
				if (mode == "transport")
				{
					throw new HttpRequestException("Synthetic transport detail");
				}
				if (mode == "timeout")
				{
					throw new TaskCanceledException("Synthetic timeout detail");
				}
				cancellation.Cancel();
				return mode == "cancel-after-401"
					? Status(HttpStatusCode.Unauthorized)
					: throw new OperationCanceledException(cancellation.Token);
			}
		};
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger);
		Assert.Equal(canceled ? CalendarSyncService.SyncCycleOutcome.Canceled : CalendarSyncService.SyncCycleOutcome.Failed,
			await service.PerformSyncAsync(cancellation.Token, false));
		Assert.Equal(["PROPFIND", "PUT"], fake.Requests.Select(request => request.Method));
		Assert.Contains($"MutationOutcome={mutationOutcome} WritePossible={possible}", Summary(logger));
		Assert.All(logger.Messages, message => Assert.DoesNotContain("Synthetic", message));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task FullResyncRebuildRequiresEmptyUnfilteredRecheck(bool remains)
	{
		var other = Member(CalendarRules.BuildManagedUid("other", "other-id", Start));
		using var fake = new FakeServer(Listing(other), Status(HttpStatusCode.NoContent), remains ? Listing(other) : Listing(),
			Status(HttpStatusCode.Created), Event(DesiredUid));
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger);
		Assert.Equal(remains ? CalendarSyncService.SyncCycleOutcome.Failed : CalendarSyncService.SyncCycleOutcome.Succeeded,
			await service.PerformSyncAsync(default, true));
		Assert.Equal(remains ? ["PROPFIND", "DELETE", "PROPFIND"] : new[] { "PROPFIND", "DELETE", "PROPFIND", "PUT", "GET" },
			fake.Requests.Select(request => request.Method));
		Assert.Contains(remains ? "Stage=WipeRecheck Reason=WipeNotEmpty" : "Stage=Complete", Summary(logger));
		Assert.Contains(remains ? "WipeDeleted=1 MutationOutcome=Partial" : "WipeDeleted=1 MutationOutcome=Confirmed", Summary(logger));
		Assert.Contains(remains ? "WipeRemaining=1" : "WipeRemaining=0", Summary(logger));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task SecondUncertainDeleteStopsAndReportsConfirmedAndPossibleDeletesSeparately(bool fullResync)
	{
		var secondUid = Uid("second-stale");
		var responses = new List<HttpResponseMessage> { Listing(Member(StaleUid), Member(secondUid), Member(Uid("third-stale"))) };
		if (!fullResync)
		{
			responses.AddRange([Event(StaleUid), Event(secondUid), Event(Uid("third-stale"))]);
		}
		responses.AddRange([Status(HttpStatusCode.NoContent), Status(HttpStatusCode.Accepted)]);
		using var fake = new FakeServer(responses.ToArray());
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger, Snapshot(null));
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, fullResync));
		Assert.Equal(fullResync ? ["PROPFIND", "DELETE", "DELETE"] : new[] { "PROPFIND", "GET", "GET", "GET", "DELETE", "DELETE" },
			fake.Requests.Select(request => request.Method));
		Assert.Contains("MutationOutcome=Partial", Summary(logger));
		Assert.Contains(fullResync ? "Deleted=0 WipeListed=3 WipeDeleted=1" : "Deleted=1 WipeListed=0 WipeDeleted=0", Summary(logger));
		Assert.Contains(fullResync ? "DeletePossible=0 WipeDeletePossible=1" : "DeletePossible=1 WipeDeletePossible=0", Summary(logger));
	}

	[Theory]
	[InlineData("continuation")]
	[InlineData("missing-collection")]
	[InlineData("duplicate")]
	[InlineData("weak")]
	[InlineData("other-collection")]
	[InlineData("partial")]
	[InlineData("unknown-type")]
	[InlineData("failed-type")]
	[InlineData("conflicting-tag")]
	[InlineData("collection-alias")]
	public async Task IncompleteOrAmbiguousListingStopsBeforeMutation(string mode)
	{
		using var listing = mode switch
		{
			"missing-collection" => Status(HttpStatusCode.MultiStatus),
			"duplicate" => Listing(Member(DesiredUid), Member(DesiredUid.ToUpperInvariant())),
			"weak" => Listing(Member(DesiredUid, tag: "W/\"listed\"")),
			"other-collection" => Listing(Member(DesiredUid, collection: "/other/")),
			"unknown-type" => Listing(PropertyResponse(Collection + DesiredUid + ".ics", new XElement(Dav + "getetag", "\"listed\""))),
			"failed-type" => Listing(WithPropertyFailure(PropertyResponse(Collection + DesiredUid + ".ics", new XElement(Dav + "getetag", "\"listed\"")), "resourcetype")),
			"conflicting-tag" => Listing(WithPropertyFailure(Member(DesiredUid), "getetag")),
			"collection-alias" => Listing(Member(DesiredUid), PropertyResponse(Collection + DesiredUid + ".ics/",
				new XElement(Dav + "resourcetype", new XElement(Dav + "collection")))),
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

	[Fact]
	public async Task SplitPropstatsAndNonEventCollectionsDoNotBlockReconciliation()
	{
		var split = PropertyResponse(BaseUrl + Collection + DesiredUid + ".ics", new XElement(Dav + "resourcetype"));
		split.Add(PropStat("HTTP/2 200", new XElement(Dav + "getetag", "\"listed\"")));
		var subcollection = WithPropertyFailure(PropertyResponse(Collection + "subcollection.ics",
			new XElement(Dav + "resourcetype", new XElement(Dav + "collection"))), "getetag");
		using var fake = new FakeServer(Listing(split, subcollection), Event(DesiredUid));
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "GET"], fake.Requests.Select(request => request.Method));
	}

	[Fact]
	public async Task FoldedCalendarLinesCanConfirmAnEquivalentEvent()
	{
		using var folded = Event(DesiredUid);
		await ChangeEventText(folded, text => text.Replace("UID:", "UI\r\n\tD:", StringComparison.Ordinal));
		using var fake = new FakeServer(Listing(Member(DesiredUid)), folded);
		using var service = CreateService(fake, new CaptureLogger());
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Succeeded, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "GET"], fake.Requests.Select(request => request.Method));
	}

	[Theory]
	[InlineData("range", "EventReadIncomplete")]
	[InlineData("missing-tag", "EventETagInvalid")]
	[InlineData("weak-tag", "EventETagInvalid")]
	[InlineData("duplicate-tag", "EventETagInvalid")]
	[InlineData("truncated", "EventMalformed")]
	[InlineData("multiple", "EventMalformed")]
	[InlineData("ambiguous-uid", "EventMalformed")]
	public async Task AmbiguousStaleOwnershipBlocksAllDeletes(string mode, string reason)
	{
		var secondUid = Uid("second-stale");
		using var bad = Event(secondUid);
		switch (mode)
		{
			case "range":
				bad.Content.Headers.ContentRange = new ContentRangeHeaderValue(0, 99, 1000);
				break;
			case "missing-tag":
				bad.Headers.ETag = null;
				break;
			case "weak-tag":
				bad.Headers.ETag = EntityTagHeaderValue.Parse("W/\"listed\"");
				break;
			case "duplicate-tag":
				bad.Headers.TryAddWithoutValidation("ETag", "\"different\"");
				break;
			default:
				await ChangeEventText(bad, text => mode switch
				{
					"truncated" => text.Replace("END:VCALENDAR", "", StringComparison.Ordinal),
					"multiple" => text + text,
					_ => text.Replace("END:VEVENT", "UID:another-synthetic-id\r\nEND:VEVENT", StringComparison.Ordinal)
				});
				break;
		}
		using var fake = new FakeServer(Listing(Member(StaleUid), Member(secondUid)), Event(StaleUid), bad);
		var logger = new CaptureLogger();
		using var service = CreateService(fake, logger, Snapshot(null));
		Assert.Equal(CalendarSyncService.SyncCycleOutcome.Failed, await service.PerformSyncAsync(default, false));
		Assert.Equal(["PROPFIND", "GET", "GET"], fake.Requests.Select(request => request.Method));
		Assert.Contains($"Stage=StaleOwnershipRead Reason={reason}", Summary(logger));
		Assert.Contains("OwnershipSkipped=0 Deleted=0", Summary(logger));
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
			logger, _ => Task.FromResult(snapshot ?? Snapshot(Desired)), () => new HttpClient(server, false),
			wipeDelay: (_, token) => Task.Delay(TimeSpan.Zero, token));

	private static string Summary(CaptureLogger logger) =>
		Assert.Single(logger.Messages, message => message.StartsWith("SyncCycle ", StringComparison.Ordinal));

	private static OutlookSnapshot Snapshot(OutlookEventDto? dto) => new(
		dto == null ? [] : new Dictionary<string, OutlookEventDto> { [DesiredUid] = dto }, true, 0, false);

	private static string Uid(string id) => CalendarRules.BuildManagedUid(Source, id, Start);

	private static readonly XNamespace Dav = "DAV:";
	private static XElement PropStat(string status, params XElement[] properties) => new(Dav + "propstat",
		new XElement(Dav + "prop", properties), new XElement(Dav + "status", status));

	private static XElement PropertyResponse(string path, params XElement[] properties) => new(Dav + "response",
		new XElement(Dav + "href", path), PropStat("HTTP/1.1 200 OK", properties));

	private static XElement WithPropertyFailure(XElement entry, string property)
	{
		entry.Add(PropStat("HTTP/1.1 404 Not Found", new XElement(Dav + property)));
		return entry;
	}

	private static XElement Member(string uid, string extension = ".ics", string collection = Collection, string tag = "\"listed\"") =>
		PropertyResponse(collection + uid + extension, new XElement(Dav + "getetag", tag), new XElement(Dav + "resourcetype"));

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

	private static async Task ChangeEventText(HttpResponseMessage response, Func<string, string> transform)
	{
		using var original = response.Content;
		var text = await original.ReadAsStringAsync();
		var changed = transform(text);
		Assert.NotEqual(text, changed);
		response.Content = new StringContent(changed);
	}

	private sealed record Request(string Method, string Path, string IfMatch, string IfNoneMatch, string? Body, bool NoCache);

	private sealed class FakeServer(params HttpResponseMessage[] responses) : HttpMessageHandler
	{
		private readonly Queue<HttpResponseMessage> _responses = new(responses);
		internal List<Request> Requests { get; } = [];
		internal Func<HttpRequestMessage, HttpResponseMessage?>? Override { get; init; }

		protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
		{
			Requests.Add(new Request(request.Method.Method, request.RequestUri!.AbsolutePath,
				request.Headers.IfMatch.ToString(), request.Headers.IfNoneMatch.ToString(),
				request.Method == HttpMethod.Put ? await request.Content!.ReadAsStringAsync(token) : null,
				request.Headers.CacheControl?.NoCache == true));
			return Override?.Invoke(request) ?? _responses.Dequeue();
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
