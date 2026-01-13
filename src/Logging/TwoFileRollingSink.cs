using System.Globalization;
using System.Text;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;

namespace CalendarSync;

public sealed class TwoFileRollingSink : ILogEventSink, IDisposable
{
	private static readonly Encoding LogEncoding = new UTF8Encoding(false);
	private readonly string _logPath;
	private readonly string _oldLogPath;
	private readonly long _fileSizeLimitBytes;
	private readonly ITextFormatter _formatter;
	private readonly object _syncRoot = new();
	private StreamWriter? _writer;
	private long _currentSize;

	public TwoFileRollingSink(string logPath, string oldLogPath, long fileSizeLimitBytes)
	{
		_logPath = logPath ?? throw new ArgumentNullException(nameof(logPath));
		_oldLogPath = oldLogPath ?? throw new ArgumentNullException(nameof(oldLogPath));
		if (fileSizeLimitBytes <= 0)
		{
			throw new ArgumentOutOfRangeException(nameof(fileSizeLimitBytes));
		}
		_fileSizeLimitBytes = fileSizeLimitBytes;
		_formatter = new MessageTemplateTextFormatter(
			"{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
			CultureInfo.InvariantCulture);
	}

	public void Emit(LogEvent logEvent)
	{
		if (logEvent == null)
		{
			return;
		}

		string formatted;
		using (var writer = new StringWriter(CultureInfo.InvariantCulture))
		{
			_formatter.Format(logEvent, writer);
			formatted = writer.ToString();
		}

		var bytesToWrite = LogEncoding.GetByteCount(formatted);

		lock (_syncRoot)
		{
			EnsureWriter();
			if (_currentSize + bytesToWrite > _fileSizeLimitBytes)
			{
				Roll();
			}

			_writer!.Write(formatted);
			_currentSize += bytesToWrite;
		}
	}

	public void Dispose()
	{
		lock (_syncRoot)
		{
			if (_writer != null)
			{
				_writer.Flush();
				_writer.Dispose();
				_writer = null;
			}
		}
	}

	private void EnsureWriter()
	{
		if (_writer != null)
		{
			return;
		}

		var directory = Path.GetDirectoryName(_logPath);
		if (!string.IsNullOrWhiteSpace(directory))
		{
			Directory.CreateDirectory(directory);
		}

		var stream = new FileStream(_logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
		_writer = new StreamWriter(stream, LogEncoding) { AutoFlush = true };
		_currentSize = stream.Length;
	}

	private void Roll()
	{
		_writer?.Flush();
		_writer?.Dispose();
		_writer = null;

		if (File.Exists(_logPath))
		{
			File.Move(_logPath, _oldLogPath, true);
		}

		EnsureWriter();
	}
}
