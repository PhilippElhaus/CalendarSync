using Xunit;

namespace CalendarSync.Tests;

public sealed class CalendarRulesTests
{
	[Fact]
	public void BuildManagedUidPreservesTheEstablishedFormat()
	{
		var uid = CalendarRules.BuildManagedUid(
			"work",
			"global-123",
			new DateTime(2026, 10, 25, 1, 30, 0, DateTimeKind.Utc));

		Assert.Equal(
			"work-outlook-19929452ccc23f1dd86e7f80bf3c326d9762d2e1f5d4eb3385c18dc833a82818-20261025T013000Z",
			uid);
	}

	[Theory]
	[InlineData("work", "work", true)]
	[InlineData("work", "WORK", true)]
	[InlineData("work", "work-east", false)]
	[InlineData("work", "work-outlook-east", false)]
	[InlineData("work", "", false)]
	[InlineData("", "", false)]
	public void ManagedUidRequiresTheExactSource(string sourceId, string eventSourceId, bool expected)
	{
		Assert.Equal(expected, CalendarRules.IsManagedUid(sourceId, CreateUid(eventSourceId, "appointment")));
	}

	[Fact]
	public void StaleSelectionNeverIncludesUnmanagedEvents()
	{
		var oldUid = CreateUid("work", "old");
		var currentUid = CreateUid("work", "current");
		var stale = CalendarRules.GetStaleManagedUids(
			"work",
			[oldUid, currentUid, CreateUid("work-east", "other"), "work-legacy-event", "personal-event"],
			[currentUid]);

		Assert.Equal([oldUid], stale);
	}

	private static string CreateUid(string sourceId, string eventId) =>
		CalendarRules.BuildManagedUid(sourceId, eventId, new DateTime(2026, 8, 30, 10, 0, 0, DateTimeKind.Utc));

	[Fact]
	public void AllDayRangeUsesAnExclusiveEndDate()
	{
		var start = new DateTime(2026, 8, 30, 0, 0, 0);
		var end = new DateTime(2026, 9, 1, 0, 0, 0);

		Assert.True(CalendarRules.DetermineAllDay(start, end, false));
		Assert.Equal((start.Date, end.Date), CalendarRules.GetAllDayDateRange(start, end));
	}

	[Fact]
	public void TimedEventKeepsTagBodyLocationAndTwoReminders()
	{
		var dto = CreateTimedDto();
		var calendarEvent = CalendarRules.CreateCalendarEvent(dto, "uid-1", "CLIENT");

		Assert.Equal("[CLIENT] Meeting", calendarEvent.Summary);
		Assert.Equal("Description", calendarEvent.Description);
		Assert.Equal("Room", calendarEvent.Location);
		Assert.False(calendarEvent.IsAllDay);
		Assert.Equal(2, calendarEvent.Alarms.Count);
		Assert.Contains(calendarEvent.Alarms, alarm => alarm.Trigger?.Duration?.ToString() == "-PT10M");
		Assert.Contains(calendarEvent.Alarms, alarm => alarm.Trigger?.Duration?.ToString() == "-PT3M");
	}

	[Fact]
	public void AllDayEventHasNoReminders()
	{
		var dto = CreateTimedDto() with
		{
			StartLocal = new DateTime(2026, 8, 30),
			EndLocal = new DateTime(2026, 8, 31),
			IsAllDay = true
		};

		var calendarEvent = CalendarRules.CreateCalendarEvent(dto, "uid-2", null);

		Assert.True(calendarEvent.IsAllDay);
		Assert.Empty(calendarEvent.Alarms);
		Assert.Equal(new DateTime(2026, 8, 31), calendarEvent.End!.Value.Date);
	}

	[Fact]
	public void BerlinDstGapIsRejectedAndAmbiguousTimeUsesSystemDefaultOffset()
	{
		var berlin = TimeZoneInfo.FindSystemTimeZoneById("Europe/Berlin");
		var invalidLocal = new DateTime(2026, 3, 29, 2, 30, 0, DateTimeKind.Unspecified);
		var ambiguousLocal = new DateTime(2026, 10, 25, 2, 30, 0, DateTimeKind.Unspecified);

		Assert.Throws<ArgumentException>(() => CalendarRules.ConvertSourceLocalToUtc(invalidLocal, berlin));
		Assert.Equal(
			new DateTime(2026, 10, 25, 1, 30, 0, DateTimeKind.Utc),
			CalendarRules.ConvertSourceLocalToUtc(ambiguousLocal, berlin));
	}

	[Fact]
	public void AllDayDatesDoNotShiftAcrossSourceTimeZones()
	{
		var local = new DateTime(2026, 9, 1);
		var utc = new DateTime(2026, 8, 31, 22, 0, 0, DateTimeKind.Utc);
		var zone = TimeZoneInfo.Utc;
		Assert.Equal((local, utc), CalendarRules.NormalizeOutlookTime(local, utc, zone, true));
		Assert.Equal((utc, utc), CalendarRules.NormalizeOutlookTime(local, utc, zone, false));
		Assert.Throws<InvalidDataException>(() => CalendarRules.NormalizeOutlookTime(default, default, zone, false));
	}

	[Fact]
	public void SnapshotWindowIncludesOverlapButNotTheExclusiveEnd()
	{
		var lower = new DateTime(2026, 8, 30);
		var upper = lower.AddDays(1);
		Assert.True(CalendarRules.OverlapsWindow(lower.AddDays(-1), lower.AddHours(5), lower, upper));
		Assert.True(CalendarRules.OverlapsWindow(upper.AddMinutes(-1), upper, lower, upper));
		Assert.True(CalendarRules.OverlapsWindow(lower, lower, lower, upper));
		Assert.False(CalendarRules.OverlapsWindow(lower.AddDays(-1), lower, lower, upper));
		Assert.False(CalendarRules.OverlapsWindow(upper, upper.AddDays(1), lower, upper));
	}

	[Fact]
	public void MultiDayTimedEventHasNoReminders()
	{
		var dto = CreateTimedDto();
		dto = dto with { EndLocal = dto.EndLocal.AddDays(1), EndUtc = dto.EndUtc.AddDays(1) };
		var calendarEvent = CalendarRules.CreateCalendarEvent(dto, "synthetic-uid", null);
		Assert.False(calendarEvent.IsAllDay);
		Assert.Empty(calendarEvent.Alarms);
	}

	[Fact]
	public void ConflictingDuplicateIsRejectedAndUnreadBodyDoesNotOverwriteKnownBody()
	{
		var dto = CreateTimedDto();
		var unread = dto with { Body = "", BodyWasRead = false };
		var events = new Dictionary<string, OutlookEventDto>();
		CalendarRules.AddSnapshotEvent(events, "synthetic-uid", unread);
		CalendarRules.AddSnapshotEvent(events, "synthetic-uid", dto);
		CalendarRules.AddSnapshotEvent(events, "synthetic-uid", unread);
		Assert.Equal(dto, events["synthetic-uid"]);
		Assert.Throws<InvalidDataException>(() => CalendarRules.AddSnapshotEvent(events, "synthetic-uid", dto with { Subject = "Different" }));
		Assert.Throws<InvalidDataException>(() => CalendarRules.BuildManagedUid("fixture", "", dto.StartUtc));
		Assert.Equal(dto, Assert.Single(events).Value);
	}

	private static OutlookEventDto CreateTimedDto() =>
		new(
			"Meeting",
			"Description",
			"Room",
			new DateTime(2026, 8, 30, 10, 0, 0),
			new DateTime(2026, 8, 30, 11, 0, 0),
			new DateTime(2026, 8, 30, 8, 0, 0, DateTimeKind.Utc),
			new DateTime(2026, 8, 30, 9, 0, 0, DateTimeKind.Utc),
			"global-id",
			false,
			true);
}
