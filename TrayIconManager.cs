using System;
using System.Drawing;
using System.Windows.Forms;

namespace CalendarSync;

public sealed class TrayIconManager : IDisposable
{
	private readonly NotifyIcon _notifyIcon;
	private readonly Icon _idleIcon;
	private readonly Icon _updateIcon;
	private readonly Icon _deleteIcon;
	private readonly ContextMenuStrip _menu;

	public TrayIconManager()
	{
		_idleIcon = new Icon("icon_idle.ico");
		_updateIcon = new Icon("icon_update.ico");
		_deleteIcon = new Icon("icon_delete.ico");

		_menu = new ContextMenuStrip();
		var exitItem = new ToolStripMenuItem("Exit");
               exitItem.Click += (_, _) => Application.Exit();
		_menu.Items.Add(exitItem);

		_notifyIcon = new NotifyIcon
		{
			Icon = _idleIcon,
			Visible = true,
			Text = "Idle...",
			ContextMenuStrip = _menu
		};
	}

	public void SetIdle()
	{
		_notifyIcon.Icon = _idleIcon;
		UpdateText("Idle...");
	}

	public void SetUpdating()
	{
		_notifyIcon.Icon = _updateIcon;
		UpdateText("Updating...");
	}

	public void SetDeleting()
	{
		_notifyIcon.Icon = _deleteIcon;
		UpdateText("Deleting...");
	}

	public void UpdateText(string text)
	{
		if (text.Length > 63)
			text = text.Substring(0, 63);
		_notifyIcon.Text = text;
	}

	public void Dispose()
	{
		_notifyIcon.Visible = false;
		_notifyIcon.Dispose();
		_menu.Dispose();
		_idleIcon.Dispose();
		_updateIcon.Dispose();
		_deleteIcon.Dispose();
	}
}