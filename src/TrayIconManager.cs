using System.Diagnostics;

namespace CalendarSync;

public sealed class TrayIconManager : IDisposable
{
	private readonly Control _dispatcher;
	private readonly NotifyIcon _notifyIcon;
	private readonly Icon _idleIcon;
	private readonly Icon _updateIcon;
	private readonly Icon _deleteIcon;
	private readonly ContextMenuStrip _menu;
	private readonly ToolStripMenuItem _fullResyncItem;
	private bool _disposed;

	public event EventHandler? ExitClicked;
	public event EventHandler? FullResyncClicked;

	public TrayIconManager()
	{
		_dispatcher = new Control();
		_ = _dispatcher.Handle;

		var baseDir = AppDomain.CurrentDomain.BaseDirectory;
		_idleIcon = new Icon(Path.Combine(baseDir, "ico", "icon_idle.ico"));
		_updateIcon = new Icon(Path.Combine(baseDir, "ico", "icon_update.ico"));
		_deleteIcon = new Icon(Path.Combine(baseDir, "ico", "icon_delete.ico"));

		_menu = new ContextMenuStrip();
		var logsItem = new ToolStripMenuItem("Logs");
		logsItem.Click += (_, _) => OpenLatestLog();
		_menu.Items.Add(logsItem);

		_fullResyncItem = new ToolStripMenuItem("Full Re-Sync");
		_fullResyncItem.Click += (_, _) =>
		{
			var confirm = MessageBox.Show(
				"This will delete all events from the iCloud calendar and start a fresh sync. Continue?",
				"Confirm Full Re-Sync",
				MessageBoxButtons.YesNo,
				MessageBoxIcon.Warning);
			if (confirm == DialogResult.Yes)
			{
				FullResyncClicked?.Invoke(this, EventArgs.Empty);
			}
		};
		_menu.Items.Add(_fullResyncItem);

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

	public void SetIdle(string text = "Idle...") =>
		Dispatch(() =>
		{
			_notifyIcon.Icon = _idleIcon;
			UpdateTextCore(text);
		});

	public void SetUpdating() =>
		Dispatch(() =>
		{
			_notifyIcon.Icon = _updateIcon;
			UpdateTextCore("Updating...");
		});

	public void SetDeleting() =>
		Dispatch(() =>
		{
			_notifyIcon.Icon = _deleteIcon;
			UpdateTextCore("Deleting...");
		});

	public void SetFailed(string text) =>
		Dispatch(() =>
		{
			_notifyIcon.Icon = _idleIcon;
			UpdateTextCore(text);
		});

	public void SetFullResyncEnabled(bool enabled) =>
		Dispatch(() => _fullResyncItem.Enabled = enabled);

	public void ShowError(string title, string message) =>
		Dispatch(() => MessageBox.Show(message, title, MessageBoxButtons.OK, MessageBoxIcon.Error));

	public void UpdateText(string text) => Dispatch(() => UpdateTextCore(text));

	public void Dispose()
	{
		if (_disposed)
		{
			return;
		}

		if (_dispatcher.InvokeRequired)
		{
			try
			{
				_dispatcher.Invoke(DisposeCore);
			}
			catch (InvalidOperationException)
			{
			}
			return;
		}

		DisposeCore();
	}

	private void Dispatch(Action action)
	{
		if (_disposed || _dispatcher.IsDisposed)
		{
			return;
		}

		if (_dispatcher.InvokeRequired)
		{
			try
			{
				_dispatcher.BeginInvoke(action);
			}
			catch (InvalidOperationException)
			{
			}
			return;
		}

		action();
	}

	private void UpdateTextCore(string text)
	{
		_notifyIcon.Text = text.Length > 63 ? text[..63] : text;
	}

	private static void OpenLatestLog()
	{
		var dir = AppDomain.CurrentDomain.BaseDirectory;
		var latest = Directory.GetFiles(dir, "sync*.log")
			.Select(path => new FileInfo(path))
			.OrderByDescending(file => file.LastWriteTimeUtc)
			.FirstOrDefault();

		if (latest != null)
		{
			Process.Start(new ProcessStartInfo(latest.FullName) { UseShellExecute = true });
		}
	}

	private void DisposeCore()
	{
		if (_disposed)
		{
			return;
		}

		_disposed = true;
		_notifyIcon.Visible = false;
		_notifyIcon.Dispose();
		_menu.Dispose();
		_idleIcon.Dispose();
		_updateIcon.Dispose();
		_deleteIcon.Dispose();
		_dispatcher.Dispose();
	}
}
