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
	[InlineData("work-outlook-abc-20260830T100000Z", true)]
	[InlineData("work-legacy-event", true)]
	[InlineData("outlook-abc-20260830T100000Z", true)]
	[InlineData("other-outlook-abc-20260830T100000Z", false)]
	[InlineData("personal-event", false)]
	public void ManagedUidRecognitionPreservesCurrentAndLegacyOwnership(string uid, bool expected)
	{
		Assert.Equal(expected, CalendarRules.IsManagedUid("work", uid));
	}

	[Fact]
	public void StaleSelectionNeverIncludesUnmanagedEvents()
	{
		var stale = CalendarRules.GetStaleManagedUids(
			"work",
			["work-outlook-old", "work-outlook-current", "personal-event"],
			["work-outlook-current"]);

		Assert.Equal(["work-outlook-old"], stale);
	}

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
