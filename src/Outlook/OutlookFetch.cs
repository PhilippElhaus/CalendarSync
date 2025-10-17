using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private Task<Dictionary<string, OutlookEventDto>> FetchOutlookEventsAsync(CancellationToken token)
	{
		var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
		cts.CancelAfter(TimeSpan.FromMinutes(2));

		return StaTask.Run(() =>
		{
			Outlook.Application? outlookApp = null;
			Outlook.NameSpace? outlookNs = null;
			Outlook.MAPIFolder? calendar = null;
			Outlook.Items? items = null;

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
						items = calendar.Items;
						_logger.LogInformation("Successfully connected to Outlook.");
						break;
					}
					catch (COMException ex) when (ex.HResult == unchecked((int)0x80080005))
					{
						retryCount++;
						_logger.LogWarning(ex, $"Failed to connect to Outlook (CO_E_SERVER_EXEC_FAILURE), retry {retryCount}/{maxRetries}.");
						CleanupOutlook(outlookApp, outlookNs, calendar, items);
						outlookApp = null;
						outlookNs = null;
						calendar = null;
						items = null;

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
						CleanupOutlook(outlookApp, outlookNs, calendar, items);
						throw;
					}
					catch (Exception ex)
					{
						retryCount++;
						_logger.LogWarning(ex, "Unexpected error connecting to Outlook, retry {Retry}/{MaxRetries}.", retryCount, maxRetries);
						CleanupOutlook(outlookApp, outlookNs, calendar, items);
						outlookApp = null;
						outlookNs = null;
						calendar = null;
						items = null;

						if (retryCount == maxRetries)
						{
							throw;
						}

						EnsureOutlookProcessReady(cts.Token);
						_logger.LogDebug("Waiting 10 seconds before retry.");
						DelayWithCancellation(TimeSpan.FromSeconds(10), cts.Token);
					}
				}

				if (items == null)
				{
					_logger.LogDebug("No connection established, exiting FetchOutlookEventsAsync.");
					return new Dictionary<string, OutlookEventDto>();
				}

				items.IncludeRecurrences = true;
				items.Sort("[Start]");

				var start = DateTime.Today.AddDays(-_config.SyncDaysIntoPast);
				var end = DateTime.Today.AddDays(_config.SyncDaysIntoFuture);

				var filter = $"[Start] <= '{end:g}' AND [End] >= '{start:g}'";
				items = items.Restrict(filter);

				_logger.LogDebug("Applied Outlook Restrict filter: {Filter}", filter);

				var allItems = new List<Outlook.AppointmentItem>();
				var count = 0;

				foreach (var item in items)
				{
					if (count++ > 5000)
					{
						_logger.LogWarning("Aborting calendar item scan after 1000 items to prevent hangs.");
						break;
					}

					try
					{
						if (item is Outlook.AppointmentItem appt)
						{
							allItems.Add(appt);
						}
					}
					catch (Exception ex)
					{
						_logger.LogDebug(ex, "Skipping calendar item due to exception.");
					}
				}

				_logger.LogInformation("Collected {Count} Outlook items after manual date filter.", allItems.Count);

				var outlookEvents = GetOutlookEventsFromList(allItems);

				_logger.LogInformation("Expanded to {Count} atomic Outlook events.", outlookEvents.Count);

				return outlookEvents;
			}
			finally
			{
				_logger.LogDebug("Cleaning up Outlook COM objects.");
				CleanupOutlook(outlookApp, outlookNs, calendar, items);
			}
		}, cts.Token);
	}
}
