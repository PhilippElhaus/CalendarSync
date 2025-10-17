using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private void EnsureOutlookProcessReady(CancellationToken token)
	{
		if (!OperatingSystem.IsWindows())
		{
			return;
		}

		try
		{
			var outlookProcesses = Process.GetProcessesByName("OUTLOOK");
			if (outlookProcesses.Length == 0)
			{
				var resolvedPath = ResolveOutlookExecutablePath();
				var useShellExecute = resolvedPath == null;
				var executable = resolvedPath ?? "outlook.exe";

				var startInfo = new ProcessStartInfo(executable)
				{
					UseShellExecute = useShellExecute,
					Arguments = "/embedding",
					WindowStyle = ProcessWindowStyle.Minimized
				};

				if (useShellExecute)
				{
					_logger.LogDebug("Starting Outlook via shell.");
				}
				else
				{
					_logger.LogDebug("Starting Outlook using resolved path '{Executable}'.", executable);
				}

				try
				{
					Process.Start(startInfo);
				}
				catch (Exception ex)
				{
					_logger.LogWarning(ex, "Unable to start Outlook using '{Executable}'.", executable);
				}
			}

			var wait = Stopwatch.StartNew();
			while (Process.GetProcessesByName("OUTLOOK").Length == 0 && wait.Elapsed < TimeSpan.FromSeconds(30))
			{
				DelayWithCancellation(TimeSpan.FromSeconds(1), token);
			}

			if (Process.GetProcessesByName("OUTLOOK").Length == 0)
			{
				_logger.LogWarning("Outlook process could not be detected after attempting to start it. Ensure Outlook is installed and registered correctly.");
				return;
			}

			if (wait.Elapsed < TimeSpan.FromSeconds(30))
			{
				DelayWithCancellation(TimeSpan.FromSeconds(2), token);
			}
		}
		catch (OperationCanceledException)
		{
			throw;
		}
		catch (Exception ex)
		{
			_logger.LogWarning(ex, "Failed to ensure Outlook process is running.");
		}
	}

	private string? ResolveOutlookExecutablePath()
	{
		if (!OperatingSystem.IsWindows())
		{
			return null;
		}

		try
		{
			using var key = Registry.LocalMachine.OpenSubKey(@"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\OUTLOOK.EXE");
			var value = key?.GetValue(string.Empty) as string;
			if (string.IsNullOrWhiteSpace(value))
			{
				return null;
			}

			if (File.Exists(value))
			{
				return value;
			}

			_logger.LogWarning("Outlook executable path '{Path}' from registry does not exist.", value);
		}
		catch (Exception ex)
		{
			_logger.LogDebug(ex, "Unable to read Outlook executable path from registry.");
		}

		return null;
	}

	private static void DelayWithCancellation(TimeSpan delay, CancellationToken token)
	{
		if (delay <= TimeSpan.Zero)
		{
			return;
		}

		var waitHandles = new[] { token.WaitHandle };
		if (WaitHandle.WaitAny(waitHandles, delay) == WaitHandle.WaitTimeout)
		{
			return;
		}

		token.ThrowIfCancellationRequested();
	}

	private void CleanupOutlook(Outlook.Application? app, Outlook.NameSpace? ns, Outlook.MAPIFolder? folder, Outlook.Items? items)
	{
		try
		{
			if (items != null)
			{
				Marshal.FinalReleaseComObject(items);
			}

			if (folder != null)
			{
				Marshal.FinalReleaseComObject(folder);
			}

			if (ns != null)
			{
				Marshal.FinalReleaseComObject(ns);
			}

			if (app != null)
			{
				Marshal.FinalReleaseComObject(app);
			}
		}
		catch
		{
			_logger.LogError("Unable to clean up Outlook COM objects.");
		}
	}
}
