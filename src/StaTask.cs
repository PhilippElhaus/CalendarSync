using System;
using System.Threading;
using System.Threading.Tasks;

namespace CalendarSync;

public static class StaTask
{
	public static Task Run(Action action, CancellationToken token)
	{
		var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var registration = token.Register(() => tcs.TrySetCanceled(token));
		var thread = new Thread(() =>
		{
			try
			{
				token.ThrowIfCancellationRequested();
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
			finally
			{
				registration.Dispose();
			}
		});
		thread.IsBackground = true;
		try
		{
			thread.SetApartmentState(ApartmentState.STA);
			thread.Start();
		}
		catch
		{
			registration.Dispose();
			throw;
		}
		return tcs.Task;
	}

	public static Task<T> Run<T>(Func<T> func, CancellationToken token) => Run(func, token, null);

	internal static Task<T> Run<T>(Func<T> func, CancellationToken token, Action? canceledBeforeStart)
	{
		var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
		var registration = token.Register(() => tcs.TrySetCanceled(token));
		var thread = new Thread(() =>
		{
			try
			{
				if (token.IsCancellationRequested)
				{
					canceledBeforeStart?.Invoke();
					throw new OperationCanceledException(token);
				}
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
			finally
			{
				registration.Dispose();
			}
		});
		thread.IsBackground = true;
		try
		{
			thread.SetApartmentState(ApartmentState.STA);
			thread.Start();
		}
		catch
		{
			registration.Dispose();
			throw;
		}
		return tcs.Task;
	}
}
