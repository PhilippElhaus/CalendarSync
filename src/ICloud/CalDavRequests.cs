using System.Net;
using Microsoft.Extensions.Logging;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private const int MaxCalDavAttempts = 2;

	private async Task<HttpResponseMessage> SendCalDavAsync(
		HttpClient client,
		Func<HttpRequestMessage> requestFactory,
		string operation,
		CancellationToken token)
	{
		for (var attempt = 1; attempt <= MaxCalDavAttempts; attempt++)
		{
			token.ThrowIfCancellationRequested();
			using var request = requestFactory();

			try
			{
				var response = await client.SendAsync(
					request,
					HttpCompletionOption.ResponseHeadersRead,
					token).ConfigureAwait(false);

				if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
				{
					response.Dispose();
					throw new UnauthorizedAccessException("iCloud authentication failed.");
				}

				if (response.IsSuccessStatusCode)
				{
					return response;
				}

				var statusCode = response.StatusCode;
				var reason = response.ReasonPhrase;
				var retryDelay = GetRetryDelay(response, attempt);
				var shouldRetry = IsTransientCalDavStatus(statusCode) && attempt < MaxCalDavAttempts;
				response.Dispose();

				if (!shouldRetry)
				{
					throw new HttpRequestException(
						$"CalDAV {operation} failed with {(int)statusCode} {reason}.",
						null,
						statusCode);
				}

				_logger.LogWarning(
					"CalDAV {Operation} attempt {Attempt}/{MaxAttempts} returned {Status}. Retrying after {DelayMs} ms.",
					operation,
					attempt,
					MaxCalDavAttempts,
					statusCode,
					retryDelay.TotalMilliseconds);
				await Task.Delay(retryDelay, token).ConfigureAwait(false);
			}
			catch (HttpRequestException ex) when (attempt < MaxCalDavAttempts && ex.StatusCode == null)
			{
				var retryDelay = TimeSpan.FromSeconds(attempt * 2);
				_logger.LogWarning(
					ex,
					"CalDAV {Operation} attempt {Attempt}/{MaxAttempts} failed at the transport layer. Retrying after {DelayMs} ms.",
					operation,
					attempt,
					MaxCalDavAttempts,
					retryDelay.TotalMilliseconds);
				await Task.Delay(retryDelay, token).ConfigureAwait(false);
			}
			catch (OperationCanceledException ex) when (!token.IsCancellationRequested)
			{
				if (attempt >= MaxCalDavAttempts)
				{
					throw new HttpRequestException($"CalDAV {operation} timed out.", ex);
				}

				var retryDelay = TimeSpan.FromSeconds(attempt * 2);
				_logger.LogWarning(
					"CalDAV {Operation} attempt {Attempt}/{MaxAttempts} timed out. Retrying after {DelayMs} ms.",
					operation,
					attempt,
					MaxCalDavAttempts,
					retryDelay.TotalMilliseconds);
				await Task.Delay(retryDelay, token).ConfigureAwait(false);
			}
		}

		throw new InvalidOperationException($"CalDAV {operation} exhausted its retry budget.");
	}

	internal static bool IsTransientCalDavStatus(HttpStatusCode statusCode) =>
		statusCode == HttpStatusCode.RequestTimeout ||
		(int)statusCode == 429 ||
		(int)statusCode >= 500;

	private static TimeSpan GetRetryDelay(HttpResponseMessage response, int attempt)
	{
		var retryAfter = response.Headers.RetryAfter;
		TimeSpan delay;

		if (retryAfter?.Delta is { } delta)
		{
			delay = delta;
		}
		else if (retryAfter?.Date is { } retryDate)
		{
			delay = retryDate - DateTimeOffset.UtcNow;
		}
		else
		{
			delay = TimeSpan.FromSeconds(attempt * 2);
		}

		if (delay < TimeSpan.Zero)
		{
			return TimeSpan.Zero;
		}

		return delay > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : delay;
	}
}
