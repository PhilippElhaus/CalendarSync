using System.Net;
using Microsoft.Extensions.Logging;

namespace CalendarSync;

internal static class CalDavRequests
{
	private const int MaxCalDavAttempts = 2;

	internal static async Task<HttpResponseMessage> SendAsync(
		HttpClient client,
		Func<HttpRequestMessage> requestFactory,
		string operation,
		CancellationToken token,
		ILogger logger,
		bool retryVerificationAccess = false,
		Action? markMutationPossible = null)
	{
		for (var attempt = 1; attempt <= MaxCalDavAttempts; attempt++)
		{
			token.ThrowIfCancellationRequested();
			using var request = requestFactory();
			var readOnly = request.Method == HttpMethod.Get || request.Method.Method == "PROPFIND";

			try
			{
				HttpResponseMessage response;
				try
				{
					response = await client.SendAsync(
						request,
						readOnly ? HttpCompletionOption.ResponseContentRead : HttpCompletionOption.ResponseHeadersRead,
						token).ConfigureAwait(false);
				}
				catch (Exception ex) when (markMutationPossible != null &&
					ex is OperationCanceledException or HttpRequestException { StatusCode: null })
				{
					markMutationPossible();
					throw;
				}

				if (response.IsSuccessStatusCode)
				{
					if (attempt > 1)
					{
						logger.LogInformation("CalDAV {Operation} succeeded on attempt {Attempt}.", operation, attempt);
					}

					return response;
				}

				var statusCode = response.StatusCode;
				if (statusCode == HttpStatusCode.RequestTimeout || (int)statusCode >= 500)
				{
					markMutationPossible?.Invoke();
				}
				var retryDelay = GetRetryDelay(response, attempt);
				// Retry one 401 before reporting it. A verification GET can also
				// retry one 403 after its PUT succeeded. Do not replay forbidden writes.
				var accessRejected = statusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden;
				var retryAccess = statusCode == HttpStatusCode.Unauthorized ||
					(statusCode == HttpStatusCode.Forbidden && retryVerificationAccess && request.Method == HttpMethod.Get);
				var shouldRetry = ((readOnly && IsTransientStatus(statusCode)) || retryAccess) && attempt < MaxCalDavAttempts;
				response.Dispose();

				if (!shouldRetry)
				{
					if (accessRejected)
					{
						throw new CalDavAccessException(operation, statusCode);
					}

					throw new HttpRequestException(
						$"CalDAV {operation} failed with HTTP {(int)statusCode} ({statusCode}).",
						null,
						statusCode);
				}

				logger.LogWarning(
					"CalDAV {Operation} attempt {Attempt}/{MaxAttempts} returned HTTP {Status}. Retrying after {DelayMs} ms.",
					operation,
					attempt,
					MaxCalDavAttempts,
					(int)statusCode,
					retryDelay.TotalMilliseconds);
				await Task.Delay(retryDelay, token).ConfigureAwait(false);
			}
			catch (HttpRequestException ex) when (readOnly && attempt < MaxCalDavAttempts && ex.StatusCode == null)
			{
				var retryDelay = TimeSpan.FromSeconds(attempt * 2);
				logger.LogWarning(
					"CalDAV {Operation} attempt {Attempt}/{MaxAttempts} failed at the transport layer. Retrying after {DelayMs} ms.",
					operation,
					attempt,
					MaxCalDavAttempts,
					retryDelay.TotalMilliseconds);
				await Task.Delay(retryDelay, token).ConfigureAwait(false);
			}
			catch (OperationCanceledException) when (!token.IsCancellationRequested)
			{
				if (!readOnly || attempt >= MaxCalDavAttempts)
				{
					throw new CalDavTimeoutException();
				}

				var retryDelay = TimeSpan.FromSeconds(attempt * 2);
				logger.LogWarning(
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

	internal static bool IsTransientStatus(HttpStatusCode statusCode) =>
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

internal sealed class CalDavTimeoutException : HttpRequestException;
