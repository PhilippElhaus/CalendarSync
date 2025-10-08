using System;
using System.Threading;
using System.Threading.Tasks;

namespace CalendarSync.src;

public static class StaTask
{
	public static Task Run(Action action, CancellationToken token)
	{
		var tcs = new TaskCompletionSource();
		var thread = new Thread(() =>
		{
			try
			{
				action();
				tcs.SetResult();
			}
			catch (OperationCanceledException oce)
			{
				tcs.SetCanceled(oce.CancellationToken);
			}
			catch (ThreadInterruptedException)
			{
				tcs.SetCanceled(token);
			}
			catch (Exception ex)
			{
				tcs.SetException(ex);
			}
		});
		thread.SetApartmentState(ApartmentState.STA);
		thread.IsBackground = true;
		thread.Start();
		token.Register(() =>
		{
			tcs.TrySetCanceled(token);
			if (thread.IsAlive)
			{
				try
				{
					thread.Interrupt();
				}
				catch
				{
				}
			}
		});
		return tcs.Task;
	}

	public static Task<T> Run<T>(Func<T> func, CancellationToken token)
	{
		var tcs = new TaskCompletionSource<T>();
		var thread = new Thread(() =>
		{
			try
			{
				var result = func();
				tcs.SetResult(result);
			}
			catch (OperationCanceledException oce)
			{
				tcs.SetCanceled(oce.CancellationToken);
			}
			catch (ThreadInterruptedException)
			{
				tcs.SetCanceled(token);
			}
			catch (Exception ex)
			{
				tcs.SetException(ex);
			}
		});
		thread.SetApartmentState(ApartmentState.STA);
		thread.IsBackground = true;
		thread.Start();
		token.Register(() =>
		{
			tcs.TrySetCanceled(token);
			if (thread.IsAlive)
			{
				try
				{
					thread.Interrupt();
				}
				catch
				{
				}
			}
		});
		return tcs.Task;
	}
}
