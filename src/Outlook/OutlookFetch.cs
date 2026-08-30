using System.Collections;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private const int MaxOutlookItemsPerSnapshot = 5000;

	private Task<OutlookSnapshot> FetchOutlookEventsAsync(CancellationToken token)
	{
		var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
		cts.CancelAfter(TimeSpan.FromMinutes(2));

		if (!_outlookComGate.Wait(0))
		{
			cts.Dispose();
			_logger.LogWarning("Previous Outlook COM operation has not exited yet; skipping this sync cycle to avoid overlapping Outlook automation.");
			throw new OutlookOperationInProgressException();
		}

		try
		{
			return StaTask.Run(() => FetchOutlookEventsOnStaThread(cts), cts.Token);
		}
		catch
		{
			_outlookComGate.Release();
			cts.Dispose();
			throw;
		}
	}

	private OutlookSnapshot FetchOutlookEventsOnStaThread(CancellationTokenSource cts)
	{
		Outlook.Application? outlookApp = null;
		Outlook.NameSpace? outlookNs = null;
		Outlook.MAPIFolder? calendar = null;
		Outlook.Items? rawItems = null;
		Outlook.Items? restrictedItems = null;
		IEnumerator? enumerator = null;
		var allItems = new List<Outlook.AppointmentItem>();
		var scanFailures = 0;
		var hitItemLimit = false;

		try
		{
			var retryCount = 0;
			const int maxRetries = 5;

			while (retryCount < maxRetries && !cts.Token.IsCancellationRequested)
			{
				try
				{
					cts.Token.ThrowIfCancellationRequested();
					_logger.LogDebug("Attempting to create Outlook.Application instance.");
					outlookApp = CreateOutlookApplication(cts.Token);
					_logger.LogDebug("Getting Outlook namespace.");
					outlookNs = outlookApp.GetNamespace("MAPI");
					_logger.LogDebug("Accessing calendar folder.");
					calendar = outlookNs.GetDefaultFolder(Outlook.OlDefaultFolders.olFolderCalendar);
					_logger.LogDebug("Retrieving calendar items.");
					rawItems = calendar.Items;
					_logger.LogInformation("Successfully connected to Outlook.");
					break;
				}
				catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
				{
					retryCount++;
					_logger.LogWarning(ex, "Failed to connect to Outlook (CO_E_SERVER_EXEC_FAILURE), retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
					CleanupOutlook(outlookApp, outlookNs, calendar, rawItems);
					outlookApp = null;
					outlookNs = null;
					calendar = null;
					rawItems = null;

					if (retryCount == maxRetries)
					{
						throw;
					}

					EnsureOutlookProcessReady(cts.Token);
					_logger.LogDebug("Waiting 10 seconds before retry.");
					DelayWithCancellation(TimeSpan.FromSeconds(10), cts.Token);
				}
				catch (OperationCanceledException)
				{
					throw;
				}
				catch (Exception ex)
				{
					retryCount++;
					_logger.LogWarning(ex, "Unexpected error connecting to Outlook, retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
					CleanupOutlook(outlookApp, outlookNs, calendar, rawItems);
					outlookApp = null;
					outlookNs = null;
					calendar = null;
					rawItems = null;

					if (retryCount == maxRetries)
					{
						throw;
					}

					EnsureOutlookProcessReady(cts.Token);
					_logger.LogDebug("Waiting 10 seconds before retry.");
					DelayWithCancellation(TimeSpan.FromSeconds(10), cts.Token);
				}
			}

			if (rawItems == null)
			{
				_logger.LogWarning("Outlook connection did not yield a calendar item collection.");
				return OutlookSnapshot.Incomplete();
			}

			rawItems.IncludeRecurrences = true;
			rawItems.Sort("[Start]");

			var start = DateTime.Today.AddDays(-_config.SyncDaysIntoPast);
			var end = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);
			var filter = $"[Start] <= '{end:g}' AND [End] >= '{start:g}'";
			restrictedItems = rawItems.Restrict(filter);

			_logger.LogDebug("Applied Outlook Restrict filter: {Filter}", filter);

			try
			{
				enumerator = restrictedItems.GetEnumerator();
				var count = 0;
				while (enumerator.MoveNext())
				{
					cts.Token.ThrowIfCancellationRequested();

					if (count >= MaxOutlookItemsPerSnapshot)
					{
						hitItemLimit = true;
						_logger.LogWarning("Aborting calendar item scan after {Limit} items to prevent hangs. Snapshot is incomplete.", MaxOutlookItemsPerSnapshot);
						break;
					}

					count++;
					object? item = null;
					var retainedAppointment = false;
					try
					{
						item = enumerator.Current;
						if (item is Outlook.AppointmentItem appointment)
						{
							allItems.Add(appointment);
							retainedAppointment = true;
						}
					}
					catch (Exception ex)
					{
						scanFailures++;
						_logger.LogWarning(ex, "Failed to collect one Outlook calendar item. Snapshot is incomplete.");
					}
					finally
					{
						if (!retainedAppointment)
						{
							ReleaseComObject(item, "calendar item");
						}
					}
				}
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception ex)
			{
				scanFailures++;
				_logger.LogWarning(ex, "Outlook item enumeration ended unexpectedly. Snapshot is incomplete.");
			}

			_logger.LogInformation("Collected {Count} Outlook items after the date filter.", allItems.Count);
			var snapshot = GetOutlookEventsFromList(allItems, cts.Token);
			var totalFailures = snapshot.FailedItemCount + scanFailures;
			var complete = snapshot.IsComplete && totalFailures == 0 && !hitItemLimit;

			_logger.LogInformation(
				"Expanded to {Count} atomic Outlook events. Complete={Complete}, FailedItems={FailedItems}.",
				snapshot.Events.Count,
				complete,
				totalFailures);

			return snapshot with
			{
				IsComplete = complete,
				FailedItemCount = totalFailures,
				HitItemLimit = hitItemLimit
			};
		}
		finally
		{
			_logger.LogDebug("Cleaning up Outlook COM objects.");
			ReleaseComObject(enumerator, "Outlook item enumerator");
			ReleaseOutlookAppointmentItems(allItems);
			if (!ReferenceEquals(restrictedItems, rawItems))
			{
				ReleaseComObject(restrictedItems, "restricted Outlook items");
			}
			CleanupOutlook(outlookApp, outlookNs, calendar, rawItems);
			_outlookComGate.Release();
			cts.Dispose();
		}
	}

	private void ReleaseOutlookAppointmentItems(IEnumerable<Outlook.AppointmentItem> appointments)
	{
		foreach (var appointment in appointments)
		{
			ReleaseComObject(appointment, "Outlook appointment");
		}
	}

	private sealed class OutlookOperationInProgressException : Exception
	{
		public OutlookOperationInProgressException()
			: base("Previous Outlook COM operation is still running.")
		{
		}
	}
}
