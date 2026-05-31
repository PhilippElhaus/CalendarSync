using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using System.Runtime.InteropServices;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private readonly record struct OutlookBodyReadResult(string Body, bool WasRead);

	private const int WscSecurityProviderAntivirus = 0x4;
	private static readonly string[] OfficeRegistryVersions = ["17.0", "16.0", "15.0", "14.0", "12.0"];
	private bool? _cachedOutlookBodyReadAllowed;
	private DateTime _cachedOutlookBodyReadAllowedAtUtc = DateTime.MinValue;
	private string _cachedOutlookBodyReadReason = string.Empty;

	[DllImport("wscapi.dll")]
	private static extern int WscGetSecurityProviderHealth(int providers, out WscSecurityProviderHealth health);

	private enum WscSecurityProviderHealth
	{
		Good = 0,
		NotMonitored = 1,
		Poor = 2,
		Snooze = 3
	}

	private OutlookBodyReadResult ReadOutlookBodyIfEnabled(Outlook.AppointmentItem appt, string context)
	{
		if (!CanReadOutlookBodyWithoutPrompt(out var reason))
		{
			_logger.LogDebug("Skipping Outlook body for {Context}: {Reason}", context, reason);
			return new OutlookBodyReadResult(string.Empty, false);
		}

		try
		{
			return new OutlookBodyReadResult(appt.Body ?? string.Empty, true);
		}
		catch (COMException ex)
		{
			_logger.LogWarning(ex, "Unable to read Outlook body for {Context}; continuing without a description.", context);
			return new OutlookBodyReadResult(string.Empty, false);
		}
	}

	private bool CanReadOutlookBodyWithoutPrompt(out string reason)
	{
		var now = DateTime.UtcNow;
		if (_cachedOutlookBodyReadAllowed.HasValue &&
			now - _cachedOutlookBodyReadAllowedAtUtc < TimeSpan.FromMinutes(1))
		{
			reason = _cachedOutlookBodyReadReason;
			return _cachedOutlookBodyReadAllowed.Value;
		}

		var allowed = EvaluateOutlookBodyReadSafety(out reason);
		_cachedOutlookBodyReadAllowed = allowed;
		_cachedOutlookBodyReadAllowedAtUtc = now;
		_cachedOutlookBodyReadReason = reason;

		if (allowed)
		{
			_logger.LogInformation("Outlook body sync enabled for this cycle: {Reason}", reason);
		}
		else
		{
			_logger.LogInformation("Outlook body sync skipped for this cycle: {Reason}", reason);
		}

		return allowed;
	}

	private bool EvaluateOutlookBodyReadSafety(out string reason)
	{
		var mode = ResolveOutlookBodySyncMode();
		switch (mode)
		{
			case OutlookBodySyncMode.Never:
			reason = "OutlookBodySyncMode is Never.";
			return false;

			case OutlookBodySyncMode.Always:
			reason = "OutlookBodySyncMode is Always.";
			return true;
		}

		if (!OperatingSystem.IsWindows())
		{
			reason = "Windows Security Center is unavailable on this OS.";
			return false;
		}

		if (TryReadPromptPolicyValue("PromptOOMAddressInformationAccess", out var promptPolicy))
		{
			if (promptPolicy == 2)
			{
				reason = "Outlook policy automatically approves address-information access.";
				return true;
			}

			reason = promptPolicy == 0
				? "Outlook policy automatically denies address-information access."
				: "Outlook policy prompts for address-information access.";
			return false;
		}

		if (TryReadObjectModelGuardValue(out var objectModelGuard))
		{
			if (objectModelGuard == 2)
			{
				reason = "Outlook ObjectModelGuard is configured to never warn.";
				return true;
			}

			if (objectModelGuard == 1)
			{
				reason = "Outlook ObjectModelGuard is configured to always warn.";
				return false;
			}
		}

		if (TryGetAntivirusHealth(out var health, out var healthReason))
		{
			if (health == WscSecurityProviderHealth.Good)
			{
				reason = "Windows Security Center reports antivirus health is good.";
				return true;
			}

			reason = $"Windows Security Center antivirus health is {health}.";
			return false;
		}

		reason = healthReason;
		return false;
	}

	private OutlookBodySyncMode ResolveOutlookBodySyncMode()
	{
		if (!string.IsNullOrWhiteSpace(_config.OutlookBodySyncMode) &&
			Enum.TryParse<OutlookBodySyncMode>(_config.OutlookBodySyncMode, true, out var parsed))
		{
			return parsed;
		}

		if (_config.IncludeOutlookBody == true)
		{
			return OutlookBodySyncMode.Always;
		}

		if (_config.IncludeOutlookBody == false)
		{
			return OutlookBodySyncMode.Never;
		}

		return OutlookBodySyncMode.WhenSafe;
	}

	private static bool TryGetAntivirusHealth(out WscSecurityProviderHealth health, out string reason)
	{
		health = WscSecurityProviderHealth.Poor;
		reason = "Unable to query Windows Security Center antivirus health.";

		try
		{
			var hr = WscGetSecurityProviderHealth(WscSecurityProviderAntivirus, out health);
			if (hr == 0)
			{
				reason = $"Windows Security Center returned antivirus health {health}.";
				return true;
			}

			reason = $"Windows Security Center health query returned HRESULT 0x{hr:X8}.";
			return false;
		}
		catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
		{
			reason = "Windows Security Center health API is unavailable.";
			return false;
		}
	}

	private static bool TryReadPromptPolicyValue(string valueName, out int value)
	{
		return TryReadOfficeSecurityDword(
			RegistryHive.CurrentUser,
			$@"Software\Policies\Microsoft\Office\{{0}}\Outlook\Security",
			valueName,
			out value) ||
		TryReadOfficeSecurityDword(
			RegistryHive.LocalMachine,
			$@"SOFTWARE\Policies\Microsoft\Office\{{0}}\Outlook\Security",
			valueName,
			out value);
	}

	private static bool TryReadObjectModelGuardValue(out int value)
	{
		return TryReadOfficeSecurityDword(
			RegistryHive.CurrentUser,
			$@"Software\Microsoft\Office\{{0}}\Outlook\Security",
			"ObjectModelGuard",
			out value) ||
		TryReadOfficeSecurityDword(
			RegistryHive.LocalMachine,
			$@"SOFTWARE\Microsoft\Office\{{0}}\Outlook\Security",
			"ObjectModelGuard",
			out value);
	}

	private static bool TryReadOfficeSecurityDword(RegistryHive hive, string keyTemplate, string valueName, out int value)
	{
		value = 0;

		foreach (var view in new[] { RegistryView.Registry64, RegistryView.Registry32 })
		{
			foreach (var version in OfficeRegistryVersions)
			{
				try
				{
					using var baseKey = RegistryKey.OpenBaseKey(hive, view);
					using var key = baseKey.OpenSubKey(string.Format(keyTemplate, version));
					var rawValue = key?.GetValue(valueName);
					if (rawValue is int intValue)
					{
						value = intValue;
						return true;
					}
				}
				catch
				{
				}
			}
		}

		return false;
	}
}
