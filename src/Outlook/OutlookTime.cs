namespace CalendarSync.src;

public partial class CalendarSyncService
{
	private OutlookEventDto EnsureEventConsistency(OutlookEventDto dto, string context)
	{
		var startUtc = dto.StartUtc == DateTime.MinValue
		? ConvertFromSourceLocalToUtc(dto.StartLocal, $"{context} start fallback UTC")
		: DateTime.SpecifyKind(dto.StartUtc, DateTimeKind.Utc);
		var endUtc = dto.EndUtc == DateTime.MinValue
		? ConvertFromSourceLocalToUtc(dto.EndLocal, $"{context} end fallback UTC")
		: DateTime.SpecifyKind(dto.EndUtc, DateTimeKind.Utc);

		var startLocal = DateTime.SpecifyKind(dto.StartLocal, DateTimeKind.Unspecified);
		var expectedStartLocal = ConvertUtcToSourceLocal(startUtc);
		if (Math.Abs((startLocal - expectedStartLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
		{
			_logger.LogWarning("Adjusted start local time for {Context}. Computed {ComputedLocal:o} but found {StoredLocal:o}.", context, expectedStartLocal, startLocal);
			startLocal = expectedStartLocal;
		}

	var endLocal = DateTime.SpecifyKind(dto.EndLocal, DateTimeKind.Unspecified);
	var expectedEndLocal = ConvertUtcToSourceLocal(endUtc);
	if (Math.Abs((endLocal - expectedEndLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
	{
		_logger.LogWarning("Adjusted end local time for {Context}. Computed {ComputedLocal:o} but found {StoredLocal:o}.", context, expectedEndLocal, endLocal);
		endLocal = expectedEndLocal;
	}

CheckTargetAlignment($"{context} start", startLocal, startUtc);
CheckTargetAlignment($"{context} end", endLocal, endUtc);

return dto with { StartLocal = startLocal, EndLocal = endLocal, StartUtc = startUtc, EndUtc = endUtc };
}

private (DateTime local, DateTime utc) NormalizeOutlookTimes(DateTime localCandidate, DateTime utcCandidate, string context)
{
	if (utcCandidate == DateTime.MinValue && localCandidate == DateTime.MinValue)
	{
		_logger.LogWarning("Outlook returned no timestamps for {Context}; leaving values unset.", context);
		return (DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Unspecified), DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc));
	}

DateTime normalizedUtc;
if (utcCandidate == DateTime.MinValue)
{
	normalizedUtc = ConvertFromSourceLocalToUtc(localCandidate, $"{context} fallback UTC");
}
else
{
	normalizedUtc = DateTime.SpecifyKind(utcCandidate, DateTimeKind.Utc);
}

var expectedLocal = ConvertUtcToSourceLocal(normalizedUtc);
DateTime normalizedLocal;
if (localCandidate == DateTime.MinValue)
{
	normalizedLocal = expectedLocal;
}
else
{
	var candidateLocal = DateTime.SpecifyKind(localCandidate, DateTimeKind.Unspecified);
	if (Math.Abs((candidateLocal - expectedLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
	{
		_logger.LogWarning("Detected timezone mismatch for {Context}: Outlook local {OutlookLocal:o} differed from computed {ComputedLocal:o}. Using UTC-derived value.", context, candidateLocal, expectedLocal);
		normalizedLocal = expectedLocal;
	}
else
{
	normalizedLocal = candidateLocal;
}
}

CheckTargetAlignment(context, normalizedLocal, normalizedUtc);

return (normalizedLocal, normalizedUtc);
}

private DateTime ConvertFromSourceLocalToUtc(DateTime local, string? context = null)
{
	var unspecifiedLocal = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
	var utc = TimeZoneInfo.ConvertTimeToUtc(unspecifiedLocal, _sourceTimeZone);
	if (!string.IsNullOrEmpty(context))
	CheckTargetAlignment(context, unspecifiedLocal, utc);
	return DateTime.SpecifyKind(utc, DateTimeKind.Utc);
}

private DateTime ConvertUtcToSourceLocal(DateTime utc, string? context = null)
{
	var specifiedUtc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
	var local = TimeZoneInfo.ConvertTimeFromUtc(specifiedUtc, _sourceTimeZone);
	var unspecifiedLocal = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
	if (!string.IsNullOrEmpty(context))
	CheckTargetAlignment(context, unspecifiedLocal, specifiedUtc);
	return unspecifiedLocal;
}

private void CheckTargetAlignment(string context, DateTime sourceLocal, DateTime utc)
{
	var specifiedUtc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
	if (_sourceTimeZone.Id.Equals(_targetTimeZone.Id, StringComparison.OrdinalIgnoreCase))
	{
		var targetLocal = TimeZoneInfo.ConvertTimeFromUtc(specifiedUtc, _targetTimeZone);
		if (Math.Abs((targetLocal - sourceLocal).TotalMinutes) > TimezoneSanityToleranceMinutes)
		_logger.LogWarning("Sanity check failed for {Context}: source timezone {SourceZone} local {SourceLocal:o} maps to {TargetLocal:o} in target timezone {TargetZone}.", context, _sourceTimeZone.Id, sourceLocal, targetLocal, _targetTimeZone.Id);
	}
}

private TimeZoneInfo ResolveTimeZone(string? timeZoneId, string role)
{
	if (string.IsNullOrWhiteSpace(timeZoneId))
	{
		_logger.LogInformation("Using local system timezone {TimeZone} for {Role} calendar.", TimeZoneInfo.Local.Id, role);
		return TimeZoneInfo.Local;
	}

try
{
	var resolved = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId.Trim());
	_logger.LogInformation("Using configured timezone {TimeZone} for {Role} calendar.", resolved.Id, role);
	return resolved;
}
catch (TimeZoneNotFoundException)
{
	_logger.LogWarning("Configured {Role} timezone '{TimeZoneId}' was not found. Falling back to local timezone {Fallback}.", role, timeZoneId, TimeZoneInfo.Local.Id);
}
catch (InvalidTimeZoneException)
{
	_logger.LogWarning("Configured {Role} timezone '{TimeZoneId}' is invalid. Falling back to local timezone {Fallback}.", role, timeZoneId, TimeZoneInfo.Local.Id);
}

return TimeZoneInfo.Local;
}
}
