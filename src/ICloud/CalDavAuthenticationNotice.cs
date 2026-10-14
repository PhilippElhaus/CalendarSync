using System.Net;

namespace CalendarSync;

internal sealed class CalDavAuthenticationNotice
{
	private static readonly TimeSpan NoticeInterval = TimeSpan.FromMinutes(30);
	private int _consecutiveAuthenticationFailures;
	private DateTimeOffset? _lastNotice;

	internal bool RecordCycle(HttpStatusCode? failureStatus, bool hadSuccessfulRequest, DateTimeOffset now)
	{
		if (failureStatus != HttpStatusCode.Unauthorized || hadSuccessfulRequest)
		{
			_consecutiveAuthenticationFailures = 0;
			return false;
		}

		if (_consecutiveAuthenticationFailures < 2)
		{
			_consecutiveAuthenticationFailures++;
		}

		if (_consecutiveAuthenticationFailures < 2 ||
			(_lastNotice is { } lastNotice && now - lastNotice < NoticeInterval))
		{
			return false;
		}

		_lastNotice = now;
		return true;
	}
}
