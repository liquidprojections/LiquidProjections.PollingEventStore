using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LiquidProjections.PollingEventStore
{
    /// <summary>
    /// Tracks the progress of an individual subscriber and allows interested parties to asynchronously
    /// await a subscriber to catch-up to a certain point. 
    /// </summary>
    internal class ProgressTracker
    {
        private readonly LogMessage logger;
        private readonly List<NotificationRequest> requests = new List<NotificationRequest>();
        private readonly object syncObject = new object();

        public ProgressTracker(long lastProcessedCheckpoint, LogMessage logger)
        {
            this.logger = logger;
            LastProcessedCheckpoint = lastProcessedCheckpoint;
        }

        public long LastProcessedCheckpoint { get; private set; }

        public async Task<bool> CatchUpUntil(long checkpoint, TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (LastProcessedCheckpoint < checkpoint)
            {
                var request = NotificationRequest.For(checkpoint);

                return await WaitForNotification(timeout, cancellationToken, request);
            }
            else
            {
                return true;
            }
        }

        public Task<bool> CatchUp(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var request = NotificationRequest.ForCatchup();

            return WaitForNotification(timeout, cancellationToken, request);
        }

        private async Task<bool> WaitForNotification(TimeSpan timeout, CancellationToken cancellationToken,
            NotificationRequest request)
        {
            logger(() => $"Wait until subscription has caught up until {request.ExpectedCheckpoint}");

            lock (syncObject)
            {
                requests.Add(request);
            }

            cancellationToken.Register(() =>
            {
                lock (syncObject)
                {
                    requests.Remove(request);
                }
            });

            bool completedInTime = await request.Completed.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            lock (syncObject)
            {
                requests.Remove(request);
            }

            return completedInTime;
        }

        public void TrackProgress(long lastProcessedCheckpoint)
        {
            LastProcessedCheckpoint = lastProcessedCheckpoint;
            Notify(lastProcessedCheckpoint);
        }

        public void TrackCatchUp()
        {
            Notify(null);
        }

        private void Notify(long? lastProcessedCheckpoint)
        {
            NotificationRequest[] notificationRequests;

            lock (syncObject)
            {
                notificationRequests = requests
                    .Where(request => HasReachedExpectation(lastProcessedCheckpoint, request.ExpectedCheckpoint))
                    .ToArray();
            }

            foreach (NotificationRequest request in notificationRequests)
            {
                logger(() =>
                    $"Notifying subscriber waiting for {request.ExpectedCheckpoint} that we processed {lastProcessedCheckpoint}");

                request.Completed.SetResult(new object());

                lock (syncObject)
                {
                    requests.Remove(request);
                }
            }
        }

        private static bool HasReachedExpectation(long? lastProcessedCheckpoint, long? expectedCheckpoint)
        {
            return (!lastProcessedCheckpoint.HasValue && !expectedCheckpoint.HasValue) ||
                   (lastProcessedCheckpoint.HasValue && expectedCheckpoint.HasValue &&
                    lastProcessedCheckpoint >= expectedCheckpoint);
        }

        /// <summary>
        /// Represents a request to complete the task identified by <see cref="Completed"/> either when a subscription reaches the checkpoint
        /// set through <see cref="ExpectedCheckpoint"/> or when the subscription caught up with all pending transactions. In that case,
        /// <see cref="ExpectedCheckpoint"/> is <c>null</c>.    
        /// </summary>
        private class NotificationRequest
        {
            public static NotificationRequest For(long checkpoint)
            {
                return new NotificationRequest(checkpoint);
            }

            public TaskCompletionSource<object> Completed { get; }

            public long? ExpectedCheckpoint { get; }

            public static NotificationRequest ForCatchup()
            {
                return new NotificationRequest(null);
            }

            private NotificationRequest(long? checkpoint)
            {
                ExpectedCheckpoint = checkpoint;
                Completed = new TaskCompletionSource<object>();
            }
        }
    }
}