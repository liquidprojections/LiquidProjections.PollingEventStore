using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;

namespace LiquidProjections.PollingEventStore
{
    internal sealed class Subscription : ISubscription
    {
        private readonly PollingEventStoreAdapter eventStoreAdapter;
        private CancellationTokenSource cancellationTokenSource;
        private readonly object syncRoot = new object();
        private bool isDisposed;
        private readonly Subscriber subscriber;
        private readonly TimeSpan pollInterval;
        private readonly LogMessage logger;
        private Task task;
        private readonly string id;
        private ProgressTracker tracker;

        public Subscription(PollingEventStoreAdapter eventStoreAdapter, long lastProcessedCheckpoint,
            Subscriber subscriber, string subscriptionId, TimeSpan pollInterval, LogMessage logger)
        {
            this.eventStoreAdapter = eventStoreAdapter;
            this.subscriber = subscriber;
            this.pollInterval = pollInterval;
            id = subscriptionId;

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            this.logger = logger ?? (_ =>
            {
            });
#else
            this.logger = _ => {};
#endif

            tracker = new ProgressTracker(lastProcessedCheckpoint, logger);
        }

        public void Start()
        {
            if (task != null)
            {
                throw new InvalidOperationException("Already started.");
            }

            lock (syncRoot)
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(Subscription));
                }

                cancellationTokenSource = new CancellationTokenSource();
                logger(() => $"Subscription {id} has been started.");

                task = Task.Run(async () =>
                    {
                        try
                        {
                            var info = new SubscriptionInfo
                            {
                                Id = id,
                                Subscription = this,
                                CancellationToken = cancellationTokenSource.Token
                            };

                            await RunAsync(info).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // Do nothing.
                        }
                        catch (Exception exception)
                        {
                            logger(() =>
                                "Polling task has failed. Event subscription has been cancelled: " +
                                exception);
                        }
                    },
                    cancellationTokenSource.Token);
            }
        }

        public Task<bool> CatchUpUntil(long checkpoint, 
            TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken))
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException($"Subscription with id {id} is already disposed");
            }

            return tracker.CatchUpUntil(checkpoint, timeout, cancellationToken);
        }
        
        public Task<bool> CatchUp(
            TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken))
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException($"Subscription with id {id} is already disposed");
            }
            
            return tracker.CatchUp(timeout, cancellationToken);
        }

        private async Task RunAsync(SubscriptionInfo info)
        {
            Page page = (tracker.LastProcessedCheckpoint > 0)
                ? await HandleFirstRequestToDetectAheadSubscribers(tracker.LastProcessedCheckpoint, info)
                : await TryGetNextPage(tracker.LastProcessedCheckpoint);

            while (!cancellationTokenSource.IsCancellationRequested)
            {
                if (page != null && !page.IsEmpty)
                {
                    await PublishToSubscriber(info, page);

                    tracker.TrackProgress(page.LastCheckpoint);
                }
                else
                {
                    tracker.TrackCatchUp();

                    await Task.Delay(pollInterval);
                }

                page = await TryGetNextPage(tracker.LastProcessedCheckpoint);
            }
        }

        private async Task<Page> HandleFirstRequestToDetectAheadSubscribers(long precedingCheckpoint, SubscriptionInfo info)
        {
            const int offsetToDetectAheadSubscriber = 1;

            long actualPrecedingCheckpoint = precedingCheckpoint;
            precedingCheckpoint = precedingCheckpoint > 0 ? precedingCheckpoint - offsetToDetectAheadSubscriber : 0;

            Transaction[] transactions = null;

            Page page = await TryGetNextPage(precedingCheckpoint).ConfigureAwait(false);
            if (page != null)
            {
                transactions = page.Transactions.ToArray();
                if (!transactions.Any())
                {
                    // SMELL: This will also get raised when you start at checkpoint 0
                    await subscriber.NoSuchCheckpoint(info).ConfigureAwait(false);
                }
                else
                {
                    transactions = transactions
                        .Where(t => t.Checkpoint > actualPrecedingCheckpoint)
                        .ToArray();

                    page = new Page(actualPrecedingCheckpoint, transactions);
                }
            }

            return page;
        }

        private async Task PublishToSubscriber(SubscriptionInfo info, Page page)
        {
            // Don't block the polling adapter from pushing transactions to other subscribers.
            await Task.Run(() => subscriber.HandleTransactions(page.Transactions, info)).ConfigureAwait(false);

            logger(() =>
                $"Subscription {id} has processed a page of size {page.Transactions.Count} " +
                $"from checkpoint {page.Transactions.First().Checkpoint} " +
                $"to checkpoint {page.LastCheckpoint}.");
        }

        private async Task<Page> TryGetNextPage(long checkpoint)
        {
            Page page = null;

            try
            {
                logger(() => $"Request Page (subscription: {id}, precedingCheckpoint: {checkpoint}).");

                page = await eventStoreAdapter.GetNextPage(checkpoint, id, cancellationTokenSource.Token)
                    .WithWaitCancellation(cancellationTokenSource.Token)
                    .ConfigureAwait(false);

                logger(() =>
                    $"Received Page (subscription: {id}, size: {page.Transactions.Count}, " +
                    $"range: {page.Transactions.First().Checkpoint}-{page.Transactions.Last().Checkpoint}.");
            }
            catch (Exception ex) when (!(ex is TaskCanceledException))
            {
                logger(() => $"Caught exception while trying to get next page: {ex}");
                // Just continue the next iteration after a small pause
            }

            return page;
        }

        public void Dispose()
        {
            Complete();
            eventStoreAdapter.Unsubscribe(this);
        }

        public void Complete()
        {
            lock (syncRoot)
            {
                if (!isDisposed)
                {
                    isDisposed = true;

                    logger(() => $"Subscription {id} is being stopped.");

                    if (cancellationTokenSource != null)
                    {
                        try
                        {
                            cancellationTokenSource.Cancel();
                        }
                        catch (AggregateException)
                        {
                            // Ignore.
                        }
                    }

                    if (task == null)
                    {
                        FinishDisposing();
                    }
                    else
                    {
                        // Wait for the task asynchronously.
                        task.ContinueWith(_ => FinishDisposing());
                    }
                }
            }
        }

        private void FinishDisposing()
        {
            cancellationTokenSource?.Dispose();

            logger(() => $"Subscription {id} has been stopped.");
        }

        public Task Disposed
        {
            get { return task; }
        }
    }
}