using Newtonsoft.Json;
using Serilog.Events;
using System.Text;
using System.Text.RegularExpressions;

namespace CalendarSync;

internal static partial class SyncConfigLoader
{
	private const int MaxSyncWindowDays = 3660;
	private const int MaxInitialWaitSeconds = 3600;
	private const int MaxSyncIntervalMinutes = 1440;

	[GeneratedRegex("^[A-Za-z0-9._-]+$", RegexOptions.CultureInvariant)]
	private static partial Regex SafeSourceIdRegex();

	public static SyncConfig Load(string configPath)
	{
		ArgumentException.ThrowIfNullOrWhiteSpace(configPath);
		if (!File.Exists(configPath))
		{
			throw new FileNotFoundException("config.json not found in the executable directory.", configPath);
		}

		var configJson = File.ReadAllText(configPath);
		var config = JsonConvert.DeserializeObject<SyncConfig>(configJson)
			?? throw new InvalidDataException("config.json does not contain a configuration object.");

		Validate(config);

		if (string.IsNullOrWhiteSpace(config.SourceId))
		{
			config.SourceId = Guid.NewGuid().ToString("N");
			WriteAtomically(configPath, JsonConvert.SerializeObject(config, Formatting.Indented));
		}

		return config;
	}

	public static void Validate(SyncConfig config)
	{
		ArgumentNullException.ThrowIfNull(config);
		Require(config.ICloudUser, nameof(config.ICloudUser));
		Require(config.ICloudPassword, nameof(config.ICloudPassword));
		Require(config.PrincipalId, nameof(config.PrincipalId));
		Require(config.WorkCalendarId, nameof(config.WorkCalendarId));
		ValidatePathSegment(config.PrincipalId!, nameof(config.PrincipalId));
		ValidatePathSegment(config.WorkCalendarId!, nameof(config.WorkCalendarId));

		if (!Uri.TryCreate(config.ICloudCalDavUrl, UriKind.Absolute, out var calDavUri) ||
			!calDavUri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
		{
			throw new InvalidDataException("ICloudCalDavUrl must be an absolute HTTPS URL.");
		}

		if (!string.IsNullOrWhiteSpace(config.SourceId) && !SafeSourceIdRegex().IsMatch(config.SourceId))
		{
			throw new InvalidDataException("SourceId can contain only letters, numbers, periods, underscores, and hyphens.");
		}

		ValidateRange(config.InitialWaitSeconds, 0, MaxInitialWaitSeconds, nameof(config.InitialWaitSeconds));
		ValidateRange(config.SyncIntervalMinutes, 1, MaxSyncIntervalMinutes, nameof(config.SyncIntervalMinutes));
		ValidateRange(config.SyncDaysIntoFuture, 0, MaxSyncWindowDays, nameof(config.SyncDaysIntoFuture));
		ValidateRange(config.SyncDaysIntoPast, 0, MaxSyncWindowDays, nameof(config.SyncDaysIntoPast));
		ValidateRange(config.RecurrenceExpansionDaysFuture, 0, MaxSyncWindowDays, nameof(config.RecurrenceExpansionDaysFuture));
		ValidateRange(config.RecurrenceExpansionDaysPast, 0, MaxSyncWindowDays, nameof(config.RecurrenceExpansionDaysPast));

		if (!Enum.TryParse<LogEventLevel>(config.LogLevel, true, out _))
		{
			throw new InvalidDataException($"LogLevel '{config.LogLevel}' is not valid.");
		}

		if (!string.IsNullOrWhiteSpace(config.OutlookBodySyncMode) &&
			!Enum.TryParse<OutlookBodySyncMode>(config.OutlookBodySyncMode, true, out _))
		{
			throw new InvalidDataException($"OutlookBodySyncMode '{config.OutlookBodySyncMode}' is not valid.");
		}

		ValidateTimeZone(config.SourceTimeZoneId, nameof(config.SourceTimeZoneId));
		ValidateTimeZone(config.TargetTimeZoneId, nameof(config.TargetTimeZoneId));
	}

	private static void Require(string? value, string name)
	{
		if (string.IsNullOrWhiteSpace(value))
		{
			throw new InvalidDataException($"{name} is required.");
		}
	}

	private static void ValidateRange(int value, int minimum, int maximum, string name)
	{
		if (value < minimum || value > maximum)
		{
			throw new InvalidDataException($"{name} must be between {minimum} and {maximum}.");
		}
	}

	private static void ValidatePathSegment(string value, string name)
	{
		if (value.IndexOfAny(['/', '\\', '?', '#']) >= 0 || value.Any(char.IsWhiteSpace))
		{
			throw new InvalidDataException($"{name} must be one unescaped CalDAV path segment.");
		}
	}

	private static void ValidateTimeZone(string? timeZoneId, string name)
	{
		if (string.IsNullOrWhiteSpace(timeZoneId))
		{
			return;
		}

		try
		{
			TimeZoneInfo.FindSystemTimeZoneById(timeZoneId.Trim());
		}
		catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
		{
			throw new InvalidDataException($"{name} '{timeZoneId}' is not a valid timezone on this host.", ex);
		}
	}

	private static void WriteAtomically(string configPath, string json)
	{
		var directory = Path.GetDirectoryName(configPath)
			?? throw new InvalidOperationException("The configuration path has no parent directory.");
		var tempPath = Path.Combine(directory, $".{Path.GetFileName(configPath)}.{Guid.NewGuid():N}.tmp");

		try
		{
			File.WriteAllText(tempPath, json, new UTF8Encoding(false));
			File.Replace(tempPath, configPath, null, true);
		}
		finally
		{
			if (File.Exists(tempPath))
			{
				File.Delete(tempPath);
			}
		}
	}
}
