using System;
using System.Collections.Generic;
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
        private long lastProcessedCheckpoint;
        private readonly Subscriber subscriber;
        private readonly TimeSpan pollInterval;
        private readonly LogMessage logger;
        private Task task;
        private readonly string id;

        public Subscription(PollingEventStoreAdapter eventStoreAdapter, long lastProcessedCheckpoint,
            Subscriber subscriber, string subscriptionId, TimeSpan pollInterval, LogMessage logger)
        {
            this.eventStoreAdapter = eventStoreAdapter;
            this.lastProcessedCheckpoint = lastProcessedCheckpoint;
            this.subscriber = subscriber;
            this.pollInterval = pollInterval;
            id = subscriptionId;

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            this.logger = logger ?? (_ =>
            {
            });
#else
            this.logger= _ => {};
#endif
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
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() => $"Subscription {id} has been started.");
#endif

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

        private async Task RunAsync(SubscriptionInfo info)
        {
            const int offsetToDetectAheadSubscriber = 1;
            long actualRequestedLastCheckpoint = lastProcessedCheckpoint;
            lastProcessedCheckpoint = lastProcessedCheckpoint > 0 ? lastProcessedCheckpoint - offsetToDetectAheadSubscriber : 0;
            bool firstRequestAfterSubscribing = true;

            while (!cancellationTokenSource.IsCancellationRequested)
            {
                Page page = await TryGetNextPage(lastProcessedCheckpoint).ConfigureAwait(false);
                if (page != null)
                {
                    IReadOnlyList<Transaction> transactions = page.Transactions;

                    if (firstRequestAfterSubscribing)
                    {
                        if (!transactions.Any())
                        {
                            await subscriber.NoSuchCheckpoint(info).ConfigureAwait(false);
                        }
                        else
                        {
                            transactions = transactions
                                .Where(t => t.Checkpoint > actualRequestedLastCheckpoint)
                                .ToReadOnlyList();
                        }

                        firstRequestAfterSubscribing = false;
                    }

                    if (transactions.Count > 0)
                    {
                        await Task.Run(() => subscriber.HandleTransactions(transactions, info)).ConfigureAwait(false);

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                        logger(() =>
                            $"Subscription {id} has processed a page of size {page.Transactions.Count} " +
                            $"from checkpoint {page.Transactions.First().Checkpoint} " +
                            $"to checkpoint {page.Transactions.Last().Checkpoint}.");
#endif

                        lastProcessedCheckpoint = page.LastCheckpoint;
                    }
                    else
                    {
                        await Task.Delay(pollInterval);
                    }
                }
                else
                {
                    await Task.Delay(pollInterval);
                }
            }
        }

        private async Task<Page> TryGetNextPage(long checkpoint)
        {
            Page page = null;

            try
            {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Request Page (subscription: {id}, precedingCheckpoint: {checkpoint}).");
#endif

                page = await eventStoreAdapter.GetNextPage(checkpoint, id, cancellationTokenSource.Token)
                    .WithWaitCancellation(cancellationTokenSource.Token)
                    .ConfigureAwait(false);

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Received Page (subscription: {id}, size: {page.Transactions.Count}, " +
                    $"range: {page.Transactions.First().Checkpoint}-{page.Transactions.Last().Checkpoint}.");
#endif
            }
            catch
            {
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

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() => $"Subscription {id} is being stopped.");
#endif

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

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            logger(() => $"Subscription {id} has been stopped.");
#endif
                }

        public Task Disposed
        {
            get { return task; }
        }
    }
}
