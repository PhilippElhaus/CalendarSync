using System.Runtime.InteropServices;
using System.Threading;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync.src;

public partial class CalendarSyncService
{
	private Outlook.Application CreateOutlookApplication(CancellationToken token)
	{
		EnsureOutlookProcessReady(token);
		Outlook.Application? application = null;
		COMException? lastServerException = null;
		const int maxAttempts = 3;

		for (var attempt = 1; attempt <= maxAttempts; attempt++)
		{
			token.ThrowIfCancellationRequested();

			application = TryGetRunningOutlookInstance();
			if (application != null)
			{
				_logger.LogDebug("Attached to running Outlook instance.");
				return application;
			}

		try
		{
			_logger.LogDebug("Attempting to create Outlook.Application instance (attempt {Attempt}/{MaxAttempts}).", attempt, maxAttempts);
			application = new Outlook.Application();
			_logger.LogDebug("Created new Outlook.Application instance.");
			return application;
		}
	catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
	{
		lastServerException = ex;
		_logger.LogWarning(ex, "Outlook.Application creation failed with CO_E_SERVER_EXEC_FAILURE, attempt {Attempt}/{MaxAttempts}.", attempt, maxAttempts);
		if (attempt == maxAttempts)
		{
			break;
		}
	DelayWithCancellation(TimeSpan.FromSeconds(5), token);
}
}

application = TryGetRunningOutlookInstance();
if (application != null)
{
	_logger.LogDebug("Attached to running Outlook instance after retry failures.");
	return application;
}

throw lastServerException ?? new COMException("Failed to create Outlook.Application instance.", unchecked((int)0x80080005));
}

private Outlook.Application? TryGetRunningOutlookInstance()
{
	try
	{
		var clsid = OutlookApplicationClsid;
		var hr = GetActiveObjectNative(ref clsid, IntPtr.Zero, out var activeObject);
		if (hr < 0)
		{
			Marshal.ThrowExceptionForHR(hr);
		}
	if (activeObject is Outlook.Application outlookApp)
	{
		return outlookApp;
	}
_logger.LogDebug("Active Outlook COM object was not of the expected type.");
return null;
}
catch (COMException ex) when (ex.HResult == unchecked((int)0x800401E3) || ex.HResult == unchecked((int)0x80040154))
{
	return null;
}
catch (Exception ex)
{
	_logger.LogDebug(ex, "Failed to attach to running Outlook instance.");
	return null;
}
}
}
