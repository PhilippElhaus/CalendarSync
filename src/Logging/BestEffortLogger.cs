using Microsoft.Extensions.Logging;

namespace CalendarSync;

internal sealed class BestEffortLogger(ILogger logger) : ILogger
{
	public IDisposable? BeginScope<TState>(TState state) where TState : notnull
	{
		try
		{
			var scope = logger.BeginScope(state);
			return scope == null ? null : new SafeScope(scope);
		}
		catch { return null; }
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		try { return logger.IsEnabled(logLevel); }
		catch { return false; }
	}

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
		Func<TState, Exception?, string> formatter)
	{
		try { logger.Log(logLevel, eventId, state, null, formatter); }
		catch { }
	}

	private sealed class SafeScope(IDisposable scope) : IDisposable
	{
		public void Dispose()
		{
			try { scope.Dispose(); }
			catch { }
		}
	}
}
