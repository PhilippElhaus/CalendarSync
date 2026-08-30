using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Ical.Net;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;

namespace CalendarSync;

internal static class CalendarRules
{
	private const double AllDayToleranceMinutes = 5;

	public static bool DetermineAllDay(DateTime startLocal, DateTime endLocal, bool flaggedAllDay)
	{
		var span = endLocal - startLocal;
		if (span <= TimeSpan.Zero)
		{
			return flaggedAllDay;
		}

		if (flaggedAllDay)
		{
			return true;
		}

		var startMinutes = Math.Abs(startLocal.TimeOfDay.TotalMinutes);
		var endTime = endLocal.TimeOfDay;
		var endMinutes = Math.Abs(endTime.TotalMinutes);
		var minutesToMidnight = Math.Abs((TimeSpan.FromDays(1) - endTime).TotalMinutes);

		return span.TotalHours >= 23 &&
			startMinutes <= AllDayToleranceMinutes &&
			(endTime == TimeSpan.Zero || endMinutes <= AllDayToleranceMinutes || minutesToMidnight <= AllDayToleranceMinutes);
	}

	public static (DateTime StartDate, DateTime EndDate) GetAllDayDateRange(DateTime startLocal, DateTime endLocal)
	{
		var startDate = startLocal.Date;
		var endLocalTime = endLocal.TimeOfDay;
		var exclusiveEnd = endLocalTime <= TimeSpan.FromMinutes(AllDayToleranceMinutes)
			? endLocal.Date
			: endLocal.Date.AddDays(1);

		if (exclusiveEnd <= startDate)
		{
			exclusiveEnd = startDate.AddDays(1);
		}

		return (startDate, exclusiveEnd);
	}

	public static string BuildManagedUid(string sourceId, string baseUid, DateTime startUtc)
	{
		var prefix = string.IsNullOrWhiteSpace(sourceId) ? "outlook" : $"{sourceId}-outlook";
		var baseKey = string.IsNullOrWhiteSpace(baseUid) ? Guid.Empty.ToString("N") : baseUid;
		var baseHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(baseKey))).ToLowerInvariant();
		var occurrenceMarker = DateTime.SpecifyKind(startUtc, DateTimeKind.Utc)
			.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
		return $"{prefix}-{baseHash}-{occurrenceMarker}";
	}

	public static bool IsManagedUid(string sourceId, string? uid)
	{
		if (string.IsNullOrWhiteSpace(uid))
		{
			return false;
		}

		var normalized = uid.Trim();
		var prefixes = new List<string>();

		if (!string.IsNullOrEmpty(sourceId))
		{
			prefixes.Add($"{sourceId}-outlook-");
		}

		prefixes.Add("-outlook-");
		prefixes.Add("outlook-");

		if (prefixes.Any(prefix => normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
		{
			return true;
		}

		// Keep compatibility with UIDs emitted before the current UID format.
		return !string.IsNullOrEmpty(sourceId) &&
			normalized.StartsWith($"{sourceId}-", StringComparison.OrdinalIgnoreCase);
	}

	public static CalendarEvent CreateCalendarEvent(OutlookEventDto appointment, string uid, string? tag)
	{
		var summary = appointment.Subject ?? "No Subject";
		if (!string.IsNullOrEmpty(tag))
		{
			summary = $"[{tag}] {summary}";
		}

		CalDateTime start;
		CalDateTime end;
		var isAllDay = DetermineAllDay(appointment.StartLocal, appointment.EndLocal, appointment.IsAllDay);

		if (isAllDay)
		{
			var (startDate, endDate) = GetAllDayDateRange(appointment.StartLocal, appointment.EndLocal);
			start = new CalDateTime(startDate, false);
			end = new CalDateTime(endDate, false);
		}
		else
		{
			start = new CalDateTime(appointment.StartUtc, CalDateTime.UtcTzId);
			end = new CalDateTime(appointment.EndUtc, CalDateTime.UtcTzId);
		}

		var calendarEvent = new CalendarEvent
		{
			Summary = summary,
			Start = start,
			End = end,
			Location = appointment.Location ?? string.Empty,
			Uid = uid,
			Description = appointment.Body ?? string.Empty
		};

		if (!isAllDay)
		{
			calendarEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT10M") });
			calendarEvent.Alarms.Add(new Alarm { Action = AlarmAction.Display, Description = "Reminder", Trigger = new Trigger("-PT3M") });
		}

		return calendarEvent;
	}

	public static DateTime ConvertSourceLocalToUtc(DateTime local, TimeZoneInfo sourceTimeZone)
	{
		var unspecifiedLocal = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
		var utc = TimeZoneInfo.ConvertTimeToUtc(unspecifiedLocal, sourceTimeZone);
		return DateTime.SpecifyKind(utc, DateTimeKind.Utc);
	}

	public static DateTime ConvertUtcToSourceLocal(DateTime utc, TimeZoneInfo sourceTimeZone)
	{
		var specifiedUtc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
		var local = TimeZoneInfo.ConvertTimeFromUtc(specifiedUtc, sourceTimeZone);
		return DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
	}

	public static List<string> GetStaleManagedUids(
		string sourceId,
		IEnumerable<string> remoteUids,
		IEnumerable<string> desiredUids)
	{
		var desired = new HashSet<string>(desiredUids, StringComparer.OrdinalIgnoreCase);
		return remoteUids
			.Where(uid => IsManagedUid(sourceId, uid) && !desired.Contains(uid))
			.ToList();
	}
}
