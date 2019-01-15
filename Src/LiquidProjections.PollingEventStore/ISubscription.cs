using System;
using System.Threading;
using System.Threading.Tasks;

namespace LiquidProjections.PollingEventStore
{
#if LIQUIDPROJECTIONS_BUILD_TIME
    public
#else
    internal 
#endif
    interface ISubscription : IDisposable
    {
        /// <summary>
        /// Returns a task that completes when the subscription has been completed disposed, including any background activity. 
        /// </summary>
        Task Disposed { get; }

        /// <summary>
        /// Creates a task that will complete either when the subscription has processed the requested <paramref name="checkpoint"/> or
        /// when the <paramref name="timeout"/> expired.
        /// </summary>
        /// <remarks>
        /// Because of Task scheduling reasons, the task may complete with a delay. Depending on when the <paramref name="cancellationToken"/>
        /// is cancelled, it may throw a <see cref="OperationCanceledException"/>.
        /// </remarks>
        Task<bool> CatchUpUntil(long checkpoint,
            TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Creates a task that will complete either when the subscription has processed all remaining transactions or
        /// when the <paramref name="timeout"/> expired.
        /// </summary>
        /// <remarks>
        /// Because of Task scheduling reasons, the task may complete with a delay. Depending on when the <paramref name="cancellationToken"/>
        /// is cancelled, it may throw a <see cref="OperationCanceledException"/>.
        /// </remarks>
        Task<bool> CatchUp(
            TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken));
    }
}