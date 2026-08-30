using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
using Outlook = Microsoft.Office.Interop.Outlook;

namespace CalendarSync;

public partial class CalendarSyncService
{
	private bool ProcessRecurringAppointment(
		Outlook.AppointmentItem appt,
		Dictionary<string, OutlookEventDto> events,
		HashSet<string> expandedRecurringIds,
		DateTime syncStart,
		DateTime syncEnd,
		CancellationToken token)
	{
		token.ThrowIfCancellationRequested();

		Outlook.AppointmentItem seriesItem = appt;
		Outlook.AppointmentItem? masterItem = null;
		var shouldReleaseMaster = false;
		var globalId = appt.GlobalAppointmentID;

		var recurrenceState = Outlook.OlRecurrenceState.olApptMaster;
		try
		{
			recurrenceState = appt.RecurrenceState;
		}
		catch (COMException ex)
		{
			_logger.LogDebug(ex, "Failed to read recurrence state for '{Subject}'. Assuming master.", appt.Subject);
		}

		if (recurrenceState != Outlook.OlRecurrenceState.olApptMaster)
		{
			(seriesItem, masterItem, shouldReleaseMaster, globalId) = ResolveMasterAppointment(appt, globalId);
		}

		if (string.IsNullOrEmpty(globalId))
		{
			globalId = appt.GlobalAppointmentID;
		}

		if (string.IsNullOrEmpty(globalId))
		{
			globalId = Guid.NewGuid().ToString();
		}

		try
		{
			if (!expandedRecurringIds.Add(globalId))
			{
				return true;
			}

			var patternStart = syncStart.AddDays(-_config.RecurrenceExpansionDaysPast);
			var patternEnd = syncEnd.AddDays(_config.RecurrenceExpansionDaysFuture);
			var expansion = ExpandRecurrenceManually(seriesItem, patternStart, patternEnd, token);
			var baseSubject = seriesItem.Subject ?? string.Empty;
			var baseBody = ReadOutlookBodyIfEnabled(seriesItem, $"recurring '{baseSubject}'");
			var baseLocation = seriesItem.Location ?? string.Empty;

			foreach (var occurrence in expansion.Occurrences)
			{
				token.ThrowIfCancellationRequested();

				if (occurrence.StartLocal < syncStart || occurrence.StartLocal > syncEnd)
				{
					continue;
				}

				var body = occurrence.BodyOverride ?? baseBody.Body;
				var bodyWasRead = occurrence.BodyOverride != null ? occurrence.BodyWasRead : baseBody.WasRead;
				var dto = new OutlookEventDto(
					occurrence.SubjectOverride ?? baseSubject,
					body,
					occurrence.LocationOverride ?? baseLocation,
					occurrence.StartLocal,
					occurrence.EndLocal,
					occurrence.StartUtc,
					occurrence.EndUtc,
					globalId,
					occurrence.IsAllDay,
					bodyWasRead);
				dto = EnsureEventConsistency(dto, $"recurring '{dto.Subject}'");
				AddEventChunks(events, globalId, dto);
			}

			return expansion.IsComplete;
		}
		finally
		{
			ReleaseIfNeeded(masterItem, shouldReleaseMaster);
		}
	}

	private (Outlook.AppointmentItem seriesItem, Outlook.AppointmentItem? masterItem, bool shouldRelease, string globalId) ResolveMasterAppointment(Outlook.AppointmentItem appt, string globalId)
	{
		Outlook.AppointmentItem? masterItem = null;
		var shouldReleaseMaster = false;
		Outlook.AppointmentItem seriesItem = appt;
		Outlook.RecurrencePattern? pattern = null;

		try
		{
			pattern = appt.GetRecurrencePattern();
			if (pattern?.Parent is Outlook.AppointmentItem parent)
			{
				masterItem = parent;
				if (!ReferenceEquals(parent, appt))
				{
					shouldReleaseMaster = true;
					seriesItem = parent;
				}
				try
				{
					if (!string.IsNullOrEmpty(parent.GlobalAppointmentID))
					{
						globalId = parent.GlobalAppointmentID;
					}
				}
				catch (COMException)
				{
				}
			}
		}
		catch (COMException ex)
		{
			_logger.LogDebug(ex, "Failed to resolve master item for '{Subject}'.", appt.Subject);
		}
		finally
		{
			ReleaseComObject(pattern, "recurrence pattern used to resolve the master");
		}

		return (seriesItem, masterItem, shouldReleaseMaster, globalId);
	}

	private void ReleaseIfNeeded(Outlook.AppointmentItem? masterItem, bool shouldReleaseMaster)
	{
		if (shouldReleaseMaster && masterItem != null)
		{
			ReleaseComObject(masterItem, "recurrence master appointment");
		}
	}
}
