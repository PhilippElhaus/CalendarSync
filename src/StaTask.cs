using System;
using System.Threading;
using System.Threading.Tasks;

namespace CalendarSync;

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
				tcs.TrySetResult();
			}
			catch (OperationCanceledException oce)
			{
				tcs.TrySetCanceled(oce.CancellationToken);
			}
			catch (ThreadInterruptedException)
			{
				tcs.TrySetCanceled(token);
			}
			catch (Exception ex)
			{
				tcs.TrySetException(ex);
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
				tcs.TrySetResult(result);
			}
			catch (OperationCanceledException oce)
			{
				tcs.TrySetCanceled(oce.CancellationToken);
			}
			catch (ThreadInterruptedException)
			{
				tcs.TrySetCanceled(token);
			}
			catch (Exception ex)
			{
				tcs.TrySetException(ex);
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
