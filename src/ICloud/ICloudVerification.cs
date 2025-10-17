using Ical.Net;
using Ical.Net.CalendarComponents;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;
using System.Text;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private async Task<bool> VerifyICloudEventAsync(HttpClient client, string eventUrl, OutlookEventDto dto, CancellationToken token)
	{
		var uid = ExtractUidFromUrl(eventUrl);
		try
		{
			var response = await client.GetAsync(eventUrl, token).ConfigureAwait(false);
			if (!response.IsSuccessStatusCode)
			{
				_logger.LogWarning("Verification skipped for UID {Uid}: GET returned {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
				return false;
			}

			var ics = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
			var calendar = Calendar.Load(ics);
			var calEvent = calendar?.Events?.FirstOrDefault();
			if (calEvent == null)
			{
				LogVerificationFailure(uid, "iCloud response contained no events");
				return false;
			}

			var expected = GetExpectedTimes(dto);
			var actual = GetActualTimes(calEvent);
			var tolerance = TimeSpan.FromMinutes(2);

			if (expected.isAllDay != actual.isAllDay)
			{
				LogVerificationFailure(uid, $"expected all-day {expected.isAllDay} but found {actual.isAllDay}");
				return false;
			}

			var matches = expected.isAllDay
				? expected.start.Date == actual.start.Date && expected.end.Date == actual.end.Date
				: IsWithinTolerance(actual.start, expected.start, tolerance) && IsWithinTolerance(actual.end, expected.end, tolerance);

			if (!matches)
			{
				LogVerificationFailure(uid, $"expected {expected.start:o}-{expected.end:o} but found {actual.start:o}-{actual.end:o}");
				return false;
			}

			_logger.LogInformation("Verification confirmed UID {Uid} matches source timings", uid);
			return true;
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Verification failed for UID {Uid}", uid);
			EventRecorder.WriteEntry($"iCloud verification failed UID {uid}", EventLogEntryType.Error);
			return false;
		}
	}

	private async Task AttemptICloudCorrectionAsync(HttpClient client, string eventUrl, string newIcs, OutlookEventDto dto, CancellationToken token)
	{
		var uid = ExtractUidFromUrl(eventUrl);
		try
		{
			_logger.LogWarning("Attempting to correct iCloud event UID {Uid} after verification mismatch", uid);
			using var request = new HttpRequestMessage(HttpMethod.Put, eventUrl)
			{
				Content = new StringContent(newIcs, Encoding.UTF8, "text/calendar")
			};
			var response = await client.SendAsync(request, token).ConfigureAwait(false);
			if (!response.IsSuccessStatusCode)
			{
				if (response.StatusCode == HttpStatusCode.Unauthorized || response.StatusCode == HttpStatusCode.Forbidden)
				{
					throw new UnauthorizedAccessException("iCloud authentication failed.");
				}

				_logger.LogError("Correction PUT failed for UID {Uid}: {Status} - {Reason}", uid, response.StatusCode, response.ReasonPhrase);
				EventRecorder.WriteEntry($"iCloud correction failed UID {uid}", EventLogEntryType.Error);
				return;
			}

			var verified = await VerifyICloudEventAsync(client, eventUrl, dto, token).ConfigureAwait(false);
			if (verified)
			{
				_logger.LogInformation("Verification succeeded after correction for UID {Uid}", uid);
			}
			else
			{
				_logger.LogError("Verification still failing after correction for UID {Uid}", uid);
				EventRecorder.WriteEntry($"iCloud verification still mismatched UID {uid}", EventLogEntryType.Error);
			}
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to correct iCloud event UID {Uid}", uid);
			EventRecorder.WriteEntry($"iCloud correction exception UID {uid}", EventLogEntryType.Error);
		}
	}

	private void LogVerificationFailure(string uid, string message)
	{
		_logger.LogError("Verification mismatch for UID {Uid}: {Message}", uid, message);
		EventRecorder.WriteEntry($"iCloud verification mismatch UID {uid}: {message}", EventLogEntryType.Error);
	}

	private (DateTime start, DateTime end, bool isAllDay) GetExpectedTimes(OutlookEventDto dto)
	{
		var isAllDay = DetermineAllDay(dto.StartLocal, dto.EndLocal, dto.IsAllDay);

		if (isAllDay)
		{
			var (startDate, endDate) = GetAllDayDateRange(dto.StartLocal, dto.EndLocal);
			return (startDate, endDate, true);
		}

		return (dto.StartUtc, dto.EndUtc, false);
	}

	private static (DateTime start, DateTime end, bool isAllDay) GetActualTimes(CalendarEvent calEvent)
	{
		var isAllDay = calEvent.IsAllDay;
		if (isAllDay)
		{
			var startDate = calEvent.Start?.Value.Date ?? DateTime.MinValue.Date;
			var endDate = calEvent.End?.Value.Date ?? startDate;
			return (startDate, endDate, true);
		}

		var start = calEvent.Start?.AsUtc ?? DateTime.MinValue;
		var end = calEvent.End?.AsUtc ?? start;
		return (start, end, false);
	}

	private static bool IsWithinTolerance(DateTime actual, DateTime expected, TimeSpan tolerance)
	{
		return Math.Abs((actual - expected).TotalMinutes) <= tolerance.TotalMinutes;
	}

	private static string ExtractUidFromUrl(string eventUrl)
	{
		var uri = new Uri(eventUrl, UriKind.RelativeOrAbsolute);
		var segments = uri.IsAbsoluteUri ? uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries) : eventUrl.Split('/', StringSplitOptions.RemoveEmptyEntries);
		var lastSegment = segments.LastOrDefault() ?? string.Empty;
		return lastSegment.Replace(".ics", string.Empty, StringComparison.OrdinalIgnoreCase);
	}
}
