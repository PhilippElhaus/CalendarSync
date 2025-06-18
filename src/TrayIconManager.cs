using System.Diagnostics;
using System.IO;

namespace CalendarSync.src;

public sealed class TrayIconManager : IDisposable
{
	private readonly NotifyIcon _notifyIcon;
	private readonly Icon _idleIcon;
	private readonly Icon _updateIcon;
	private readonly Icon _deleteIcon;
	private readonly ContextMenuStrip _menu;

	public event EventHandler? ExitClicked;

        public TrayIconManager()
        {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                _idleIcon = new Icon(Path.Combine(baseDir, "ico", "icon_idle.ico"));
                _updateIcon = new Icon(Path.Combine(baseDir, "ico", "icon_update.ico"));
                _deleteIcon = new Icon(Path.Combine(baseDir, "ico", "icon_delete.ico"));

		_menu = new ContextMenuStrip();
		var logsItem = new ToolStripMenuItem("Logs");
		logsItem.Click += (_, _) =>
		{
			var dir = AppDomain.CurrentDomain.BaseDirectory;
			string? latest = null;
			var last = DateTime.MinValue;
			foreach (var file in Directory.GetFiles(dir, "sync*.log"))
			{
				var time = File.GetLastWriteTimeUtc(file);
				if (time > last)
				{
					last = time;
					latest = file;
				}
			}
			if (latest != null)
				Process.Start(new ProcessStartInfo(latest) { UseShellExecute = true });
		};
		_menu.Items.Add(logsItem);
		
		var exitItem = new ToolStripMenuItem("Exit");
		exitItem.Click += (_, _) => ExitClicked?.Invoke(this, EventArgs.Empty);
		_menu.Items.Add(exitItem);

		_notifyIcon = new NotifyIcon
		{
			Icon = _idleIcon,
			Visible = true,
			Text = "Waiting to Start...",
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