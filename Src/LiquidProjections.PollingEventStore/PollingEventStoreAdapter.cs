using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;

namespace LiquidProjections.PollingEventStore
{
    /// <summary>
    /// An adapter to a <see cref="IPassiveEventStore"/> that efficiently supports multiple concurrent subscribers
    /// each interested in a different checkpoint, without hitting the event store concurrently. 
    /// </summary>
    /// <remarks>
    /// If the implementation of <see cref="IPassiveEventStore"/> implements <see cref="IDisposable"/>, disposing 
    /// the <see cref="PollingEventStoreAdapter"/> will also dispose the event store. More diagnostic information can be logged
    /// when the <c>LIQUIDPROJECTIONS_DIAGNOSTICS</c> compiler symbol is enabled.
    /// </remarks>
#if LIQUIDPROJECTIONS_BUILD_TIME
    public
#else
    internal 
#endif
        class PollingEventStoreAdapter : IDisposable
    {
        private readonly TimeSpan pollInterval;
        private readonly int maxPageSize;
        private readonly LogMessage logger;
        private readonly IPassiveEventStore eventStore;
        internal readonly HashSet<Subscription> Subscriptions = new HashSet<Subscription>();
        private volatile bool isDisposed;
        internal readonly object SubscriptionLock = new object();
        private Task<Page> currentPreloader;

        /// <summary>
        /// Stores cached transactions by the checkpoint of their PRECEDING transaction. This ensures that
        /// gaps in checkpoints will not result in cache misses.  
        /// </summary>
        private readonly LruCache<long, Transaction> cachedTransactionsByPrecedingCheckpoint;

        /// <summary>
        /// Creates an adapter that observes an implementation of <see cref="IPassiveEventStore"/> and efficiently handles
        /// multiple subscribers.
        /// </summary>
        /// <param name="eventStore">
        /// The persistency implementation that the NEventStore is configured with.
        /// </param>
        /// <param name="cacheSize">
        /// The size of the LRU cache that will hold transactions already loaded from the event store. The larger the cache, 
        /// the higher the chance multiple subscribers can reuse the same transactions without hitting the underlying event store.
        /// Set to <c>0</c> to disable the cache alltogether.
        /// </param>
        /// <param name="pollInterval">
        /// The amount of time to wait before polling again after the event store has not yielded any transactions anymore.
        /// </param>
        /// <param name="maxPageSize">
        /// The size of the page of transactions the adapter should load from the event store for every query.
        /// </param>
        /// <param name="getUtcNow">
        /// Obsolete. Only kept in there to prevent breaking changes.
        /// </param>
        public PollingEventStoreAdapter(IPassiveEventStore eventStore, int cacheSize, TimeSpan pollInterval, int maxPageSize, Func<DateTime> getUtcNow, LogMessage logger = null)
        {
            this.eventStore = eventStore;
            this.pollInterval = pollInterval;
            this.maxPageSize = maxPageSize;
            this.logger = logger ?? (_ => {});

            if (cacheSize > 0)
            {
                cachedTransactionsByPrecedingCheckpoint = new LruCache<long, Transaction>(cacheSize);
            }
        }

        public IDisposable Subscribe(long? lastProcessedCheckpoint, Subscriber subscriber, string subscriptionId)
        {
            if (subscriber == null)
            {
                throw new ArgumentNullException(nameof(subscriber));
            }

            Subscription subscription;

            lock (SubscriptionLock)
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(typeof(PollingEventStoreAdapter).FullName);
                }

                subscription = new Subscription(this, lastProcessedCheckpoint ?? 0, subscriber, subscriptionId, pollInterval, logger);
                Subscriptions.Add(subscription);
            }

            subscription.Start();
            return subscription;
        }

        internal async Task<Page> GetNextPage(long lastProcessedCheckpoint, string subscriptionId)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(typeof(PollingEventStoreAdapter).FullName);
            }

            Page pageFromCache = TryGetNextPageFromCache(lastProcessedCheckpoint, subscriptionId);
            if (pageFromCache.Transactions.Count > 0)
            {
                return pageFromCache;
            }


            Page loadedPage = await LoadNextPage(lastProcessedCheckpoint, subscriptionId).ConfigureAwait(false);
 
            if (loadedPage.Transactions.Count == maxPageSize)
            {
                StartPreloadingNextPage(loadedPage.LastCheckpoint, subscriptionId);
            }

            return loadedPage;
        }

        private Page TryGetNextPageFromCache(long precedingCheckpoint, string subscriptionId)
        {
            if ((cachedTransactionsByPrecedingCheckpoint != null) && cachedTransactionsByPrecedingCheckpoint.TryGet(precedingCheckpoint, out Transaction cachedTransaction))
            {
                var resultPage = new List<Transaction>(maxPageSize) { cachedTransaction };

                while (resultPage.Count < maxPageSize)
                {
                    long lastCheckpoint = cachedTransaction.Checkpoint;

                    if (cachedTransactionsByPrecedingCheckpoint.TryGet(lastCheckpoint, out cachedTransaction))
                    {
                        resultPage.Add(cachedTransaction);
                    }
                    else
                    {
                        StartPreloadingNextPage(lastCheckpoint, subscriptionId);
                        break;
                    }
                }

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Subscription {subscriptionId} has found a page of size {resultPage.Count} " +
                    $"from checkpoint {resultPage.First().Checkpoint} " +
                    $"to checkpoint {resultPage.Last().Checkpoint} in the cache.");
#endif

                return new Page(precedingCheckpoint, resultPage);
            }

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            logger(() =>
                $"Subscription {subscriptionId} has not found the next transaction in the cache.");
#endif

            return new Page(precedingCheckpoint, new Transaction[0]);
        }

        private void StartPreloadingNextPage(long precedingCheckpoint, string subscriptionId)
        {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
            logger(() =>
                $"Subscription {subscriptionId} has started preloading transactions " +
                $"after checkpoint {precedingCheckpoint}.");
#endif

            // Ignore result.
            Task _ = LoadNextPage(precedingCheckpoint, subscriptionId);
        }

        private async Task<Page> LoadNextPage(long precedingCheckpoint, string subscriptionId)
        {
            while (true)
            {
                if (isDisposed)
                {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() =>
                        $"Page loading for subscription {subscriptionId} cancelled because the adapter is disposed.");
#endif

                    throw new OperationCanceledException();
                }

                Page candidatePage = await TryLoadNextOrWaitForPreLoadingToFinish(precedingCheckpoint, subscriptionId)
                    .ConfigureAwait(false);

                if (candidatePage.PrecedingCheckpoint == precedingCheckpoint)
                {
                    return candidatePage;
                }
            }
        }

        private Task<Page> TryLoadNextOrWaitForPreLoadingToFinish(long precedingCheckpoint, string subscriptionId)
        {
            if (isDisposed)
            {
                return Task.FromResult(new Page(precedingCheckpoint, new Transaction[0]));
            }

            TaskCompletionSource<Page> ourPreloader = null;
            bool isOurPreloader = false;
            Task<Page> loader = Volatile.Read(ref currentPreloader);

            try
            {
                if (loader == null)
                {
                    ourPreloader = new TaskCompletionSource<Page>();
                    Task<Page> oldLoader = Interlocked.CompareExchange(ref currentPreloader, ourPreloader.Task, null);
                    isOurPreloader = (oldLoader == null);
                    loader = isOurPreloader ? ourPreloader.Task : oldLoader;
                }

                return loader;
            }
            finally
            {
                if (isOurPreloader)
                {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() => $"Subscription {subscriptionId} created a loader {loader.Id} " +
                                     $"for a page after checkpoint {precedingCheckpoint}.");
#endif

                    if (isDisposed)
                    {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                        logger(() => $"The loader {loader.Id} is cancelled because the adapter is disposed.");
#endif
                        
                        // If the adapter is disposed before the current task is set, we cancel the task
                        // so we do not touch the event store. 
                        ourPreloader.SetCanceled();
                    }
                    else
                    {
                        // Ignore result.
                        Task _ = TryLoadNextPageAndMakeLoaderComplete(precedingCheckpoint, ourPreloader, subscriptionId);
                    }
                }
                else
                {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() => $"Subscription {subscriptionId} is waiting for loader {loader.Id}.");
#endif
                }
            }
        }

        private async Task TryLoadNextPageAndMakeLoaderComplete(long precedingCheckpoint,
            TaskCompletionSource<Page> loaderCompletionSource, string subscriptionId)
        {
            Page nextPage;

            try
            {
                try
                {
                    nextPage = await LoadAndCachePage(precedingCheckpoint, subscriptionId).ConfigureAwait(false);
                }
                finally
                {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() =>
                        $"Loader for subscription {subscriptionId} is no longer the current one.");
#endif
                    Volatile.Write(ref currentPreloader, null);
                }
            }
            catch (Exception exception)
            {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() => $"Loader for subscription {subscriptionId} has failed: " + exception);
#endif

                loaderCompletionSource.SetException(exception);
                return;
            }

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            logger(() =>
                $"Loader for subscription {subscriptionId} has completed.");
#endif
            loaderCompletionSource.SetResult(nextPage);
        }

        private async Task<Page> LoadAndCachePage(long precedingCheckpoint, string subscriptionId)
        {
            // Maybe it's just loaded to cache.
            try
            {
                Page cachedPage = TryGetNextPageFromCache(precedingCheckpoint, subscriptionId);
                if (cachedPage.Transactions.Count > 0)
                {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() => $"Loader for subscription {subscriptionId} has found a page in the cache.");
#endif
                    return cachedPage;
                }
            }
            catch (Exception exception)
            {
                logger(() => 
                        $"Failed getting transactions after checkpoint {precedingCheckpoint} from the cache: " + exception);
            }

            List<Transaction> transactions;

            try
            {
                transactions = await Task
                    .Run(() => eventStore
                        .GetFrom((precedingCheckpoint == 0) ? (long?)null : precedingCheckpoint)
                        .Take(maxPageSize)
                        .ToList())
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                logger(() => $"Failed loading transactions after checkpoint {precedingCheckpoint} from NEventStore: " +
                        exception);

                throw;
            }
            
            if (transactions.Count > 0)
            {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Loader for subscription {subscriptionId ?? "without ID"} has loaded {transactions.Count} transactions " +
                    $"from checkpoint {transactions.First().Checkpoint} to checkpoint {transactions.Last().Checkpoint}.");
#endif

                if (transactions.First().Checkpoint <= precedingCheckpoint)
                {
                    throw new InvalidOperationException(
                        $"The event store returned a transaction with checkpoint {transactions.First().Checkpoint} that is supposed to be higher than the requested {precedingCheckpoint}");
                }

                if (cachedTransactionsByPrecedingCheckpoint != null)
                {
                    /* Add to cache in reverse order to prevent other projectors
                        from requesting already loaded transactions which are not added to cache yet. */
                    for (int index = transactions.Count - 1; index > 0; index--)
                    {
                        cachedTransactionsByPrecedingCheckpoint.Set(transactions[index - 1].Checkpoint, transactions[index]);
                    }

                    cachedTransactionsByPrecedingCheckpoint.Set(precedingCheckpoint, transactions[0]);

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() =>
                        $"Loader for subscription {subscriptionId ?? "without ID"} has cached {transactions.Count} transactions " +
                        $"from checkpoint {transactions.First().Checkpoint} to checkpoint {transactions.Last().Checkpoint}.");
#endif
                }
            }
            else
            {
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Loader for subscription {subscriptionId} has discovered " +
                    $"that there are no new transactions yet. Next request for the new transactions will be delayed.");
#endif
            }

            return new Page(precedingCheckpoint, transactions);
        }

        public void Dispose()
        {
            lock (SubscriptionLock)
            {
                if (!isDisposed)
                {
                    isDisposed = true;

                    foreach (Subscription subscription in Subscriptions.ToArray())
                    {
                        subscription.Complete();
                    }

                    // New loading tasks are no longer started at this point.
                    // After the current loading task is finished, the event store is no longer used and can be disposed.
                    Task loaderToWaitFor = Volatile.Read(ref currentPreloader);

                    try
                    {
                        loaderToWaitFor?.Wait();
                    }
                    catch (AggregateException)
                    {
                        // Ignore.
                    }

                    (eventStore as IDisposable)?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Represents an event store which does not actively push transactions to LiquidProjections and which 
    /// requires polling.
    /// </summary>
#if LIQUIDPROJECTIONS_BUILD_TIME
    public
#else
    internal 
#endif 
        interface IPassiveEventStore
    {
        /// <summary>
        /// Loads <see cref="Transaction"/>s from the storage in the order that they should be projected (should be the same order that they were persisted).
        /// </summary>
        /// <remarks>
        /// The implementation is allowed to return just a limited subset of items at a time.
        /// It is up to the implementation to decide how many items should be returned at a time.
        /// The only requirement is that the implementation should return at least one <see cref="Transaction"/> 
        /// if there are any transactions having checkpoint (<see cref="Transaction.Checkpoint"/>) bigger than given one.
        /// However, the checkpoint of the first transaction must be larger than 0.
        /// </remarks>
        /// <param name="previousCheckpoint">
        ///  Determines the value of the  <see cref="Transaction.Checkpoint"/>, next to which <see cref="Transaction"/>s should be loaded from the storage.
        /// </param>
        IEnumerable<Transaction> GetFrom(long? previousCheckpoint);
    }

    /// <summary>
    /// Defines a method that can be used to route logging to the logging framework of your choice.
    /// </summary>
#if LIQUIDPROJECTIONS_BUILD_TIME
    public
#else
    internal 
#endif
        delegate void LogMessage(Func<string> messageFunc);
}
