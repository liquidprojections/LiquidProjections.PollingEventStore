using System;
using System.Threading;
using System.Threading.Tasks;

namespace LiquidProjections.PollingEventStore
{
    internal static class TaskExtensions
    {
        public static Task<TResult> WithWaitCancellation<TResult>(this Task<TResult> task,
            CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<TResult>();
            var registration = cancellationToken.Register(s =>
            {
                var source = (TaskCompletionSource<TResult>) s;
                source.TrySetCanceled();
            }, tcs);

            task.ContinueWith((t, s) =>
            {
                var tcsAndRegistration = (Tuple<TaskCompletionSource<TResult>, CancellationTokenRegistration>) s;

                if (t.IsFaulted && t.Exception!= null)
                {
                    tcsAndRegistration.Item1.TrySetException(t.Exception.InnerException);
                }

                if (t.IsCanceled)
                {
                    tcsAndRegistration.Item1.TrySetCanceled();
                }

                if (t.IsCompleted)
                {
                    tcsAndRegistration.Item1.TrySetResult(t.Result);
                }

                tcsAndRegistration.Item2.Dispose();
            }, Tuple.Create(tcs, registration), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            return tcs.Task;
        }

        /// <summary>
        /// Waits for a task to complete, but not longer than the specified <paramref name="timeout"/>.
        /// </summary>
        /// <returns>
        /// Returns <c>true</c> if the long-running operation completed. Returns <c>false</c>
        /// if the wait timed out. Throws a <see cref="OperationCanceledException"/> if the <paramref name="cancellationToken"/>
        /// was triggered.
        /// </returns>
        public static async Task<bool> WaitAsync(this Task longOperation, TimeSpan timeout, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (timeout != default(TimeSpan))
            {
                var delayCancellationSource = new CancellationTokenSource();

                Task delay = Task.Delay(timeout, 
                    CancellationTokenSource.CreateLinkedTokenSource(delayCancellationSource.Token, cancellationToken).Token);

                Task completedTask = await Task.WhenAny(longOperation, delay);
                if (completedTask == longOperation)
                {
                    delayCancellationSource.Cancel();

                    await longOperation;  // Very important in order to propagate exceptions

                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                await longOperation;

                return true;
            }
        } 
    }
}
