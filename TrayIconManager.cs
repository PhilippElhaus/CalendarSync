using System;
using System.Drawing;
using System.Windows.Forms;

namespace CalendarSync
{
	public sealed class TrayIconManager : IDisposable
	{
		private readonly NotifyIcon _notifyIcon;
		private readonly Icon _idleIcon;
		private readonly Icon _updateIcon;
		private readonly Icon _deleteIcon;

		public TrayIconManager()
		{
			_idleIcon = new Icon("icon_idle.ico");
			_updateIcon = new Icon("icon_update.ico");
			_deleteIcon = new Icon("icon_delete.ico");

			_notifyIcon = new NotifyIcon
			{
				Icon = _idleIcon,
				Visible = true,
				Text = "CalendarSync"
			};
		}

		public void SetIdle() => _notifyIcon.Icon = _idleIcon;

		public void SetUpdating() => _notifyIcon.Icon = _updateIcon;

		public void SetDeleting() => _notifyIcon.Icon = _deleteIcon;

		public void Dispose()
		{
			_notifyIcon.Visible = false;
			_notifyIcon.Dispose();
			_idleIcon.Dispose();
			_updateIcon.Dispose();
			_deleteIcon.Dispose();
		}
	}
}