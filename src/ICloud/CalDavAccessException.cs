using System.Net;

namespace CalendarSync;

internal sealed class CalDavAccessException : HttpRequestException
{
	internal string Operation { get; }

	internal CalDavAccessException(string operation, HttpStatusCode statusCode)
		: base($"CalDAV {operation} was rejected with HTTP {(int)statusCode} ({statusCode}).", null, statusCode)
	{
		Operation = operation;
	}
}
