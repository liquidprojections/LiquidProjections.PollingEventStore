using System;
using System.Collections.Concurrent;
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
        private readonly LogMessage logDebug;
        private readonly LogMessage logError;
        private readonly IPassiveEventStore eventStore;
        private readonly HashSet<Subscription> subscriptions = new HashSet<Subscription>();
        private readonly object subscriptionLock = new object();
        private Task loader;

        private TaskCompletionSource<bool> requestQueued = new TaskCompletionSource<bool>();

        private readonly ConcurrentQueue<LoadRequest> pendingRequests =
            new ConcurrentQueue<LoadRequest>();

        private readonly CancellationTokenSource disposingCancellationTokenSource = new CancellationTokenSource();

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
        /// <param name="logger">
        /// An optional method for logging internal diagnostic messages as well as any exceptions that happen.
        /// </param>
        /// <remarks>
        /// Diagnostic information is only logged if this code is compiled with the LIQUIDPROJECTIONS_DIAGNOSTICS compiler symbol.
        /// </remarks>
        public PollingEventStoreAdapter(IPassiveEventStore eventStore, int cacheSize, TimeSpan pollInterval, int maxPageSize,
            Func<DateTime> getUtcNow, LogMessage logger = null)
        {
            this.eventStore = eventStore;
            this.pollInterval = pollInterval;
            this.maxPageSize = maxPageSize;

#if LIQUIDPROJECTIONS_DIAGNOSTICS
            this.logDebug = logger ?? (_ =>
            {
            });
#else
            this.logDebug = _ => {};
#endif
            this.logError = logger ?? (_ =>
            {
            });

            if (cacheSize > 0)
            {
                cachedTransactionsByPrecedingCheckpoint = new LruCache<long, Transaction>(cacheSize);
            }
        }

        public ISubscription Subscribe(long? lastProcessedCheckpoint, Subscriber subscriber, string subscriptionId)
        {
            if (subscriber == null)
            {
                throw new ArgumentNullException(nameof(subscriber));
            }

            Subscription subscription;

            lock (subscriptionLock)
            {
                if (disposingCancellationTokenSource.IsCancellationRequested)
                {
                    throw new ObjectDisposedException(typeof(PollingEventStoreAdapter).FullName);
                }

                if (loader == null)
                {
                    logDebug(() => $"Queue Processing Started (subscription: {subscriptionId})");

                    ProcessPendingRequestsAsync();
                }

                subscription = new Subscription(this, lastProcessedCheckpoint ?? 0, subscriber, subscriptionId ?? "No Id",
                    pollInterval, logDebug);
                subscriptions.Add(subscription);
            }

            subscription.Start();
            return subscription;
        }

        internal void Unsubscribe(Subscription subscription)
        {
            lock (subscriptionLock)
            {
                subscriptions.Remove(subscription);
            }
        }

        internal async Task<Page> GetNextPage(long precedingCheckpoint, string subscriptionId,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            Page pageFromCache = TryGetNextPageFromCache(precedingCheckpoint, subscriptionId, cancellationToken);
            if (!pageFromCache.IsEmpty)
            {
                return pageFromCache;
            }

            LoadRequest pendingRequest =
                pendingRequests.ToArray().SingleOrDefault(x => x.PrecedingCheckpoint == precedingCheckpoint);

            if (pendingRequest != null)
            {
                logDebug(() =>
                    $"Pending Request Found (subscription: {subscriptionId}, originalSubscription: " +
                    $"{pendingRequest.SubscriptionId}, preceding checkpoint: {precedingCheckpoint}");

                return await pendingRequest.Page;
            }

            Page completedPage = await RequestPageLoad(subscriptionId, precedingCheckpoint, cancellationToken);
            if ((completedPage != null) && ShouldPreloadNextPage(completedPage))
            {
                // Ignore the result. It will become available through the queue.
                Task _ = RequestPageLoad(subscriptionId, completedPage.LastCheckpoint, cancellationToken);
            }

            return completedPage;
        }

        private bool ShouldPreloadNextPage(Page completedPage)
        {
            // NOTE: Only preload when caching is enabled. If it is not, other subscribers will only benefit from preloading if the request for a particular
            // page is still in the queue.
            return IsCachingEnabled && (completedPage.Transactions.Count == maxPageSize);
        }

        private Page TryGetNextPageFromCache(long precedingCheckpoint, string subscriptionId, CancellationToken cancellationToken)
        {
            if (IsCachingEnabled && cachedTransactionsByPrecedingCheckpoint.TryGet(precedingCheckpoint, out Transaction cachedTransaction))
            {
                var resultPage = new List<Transaction>(maxPageSize) {cachedTransaction};

                while (resultPage.Count < maxPageSize)
                {
                    long lastCheckpoint = cachedTransaction.Checkpoint;

                    if (cachedTransactionsByPrecedingCheckpoint.TryGet(lastCheckpoint, out cachedTransaction))
                    {
                        resultPage.Add(cachedTransaction);
                    }
                    else
                    {
                        Task _ = RequestPageLoad(subscriptionId, lastCheckpoint, cancellationToken);
                        break;
                    }
                }

                logDebug(() =>
                    $"Cache Hit: (subscription: {subscriptionId}, precedingCheckpoint: {precedingCheckpoint}, page size: {resultPage.Count}, range: {resultPage.First().Checkpoint}-{resultPage.Last().Checkpoint})");

                return new Page(precedingCheckpoint, resultPage);
            }

            logDebug(() =>
                $"Cache Miss: (subscription: {subscriptionId}, precedingCheckpoint: {precedingCheckpoint}).");

            return new Page(precedingCheckpoint, new Transaction[0]);
        }

        private Task<Page> RequestPageLoad(string subscriptionId, long precedingCheckpoint, CancellationToken cancellationToken)
        {
            var loadRequest = new LoadRequest
            {
                SubscriptionId = subscriptionId,
                PrecedingCheckpoint = precedingCheckpoint,
                CancellationToken = cancellationToken
            };
            
            pendingRequests.Enqueue(loadRequest);
            requestQueued.TrySetResult(true);

            return loadRequest.Page;
        }

        private void ProcessPendingRequestsAsync()
        {
            loader = Task.Run(async () =>
            {
                try
                {
                    while (!disposingCancellationTokenSource.IsCancellationRequested)
                    {
                        await WaitForPendingRequest();

                        // Keep the request on the queue, so the subscriber can wait the result of preloading the next page.
                        if (pendingRequests.TryPeek(out LoadRequest request))
                        {
                            try
                            {
                                Page page = await LoadPage(request.PrecedingCheckpoint, request.SubscriptionId, request.CancellationToken);
                                if (!page.IsEmpty)
                                {
                                    CachePage(page, request.SubscriptionId);
                                }

                                request.SetResult(page);
                            }
                            catch
                            {
                                // Ignore the exception and have the subscriber try again if it wants to.
                                request.SetResult(null);
                            }
                            finally
                            {
                                // By now, the result is available in the cache, so subscribers that were too late to await
                                // the preload can benefit from it as well.
                                pendingRequests.TryDequeue(out LoadRequest _);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // The cancellation token was triggered, so let this path die off silently.                  
                }
            }, disposingCancellationTokenSource.Token);
        }

        private async Task WaitForPendingRequest()
        {
            if (pendingRequests.IsEmpty)
            {
                await requestQueued.Task.WithWaitCancellation(disposingCancellationTokenSource.Token);
                if (pendingRequests.IsEmpty)
                {
                    requestQueued = new TaskCompletionSource<bool>();

                    await requestQueued.Task.WithWaitCancellation(disposingCancellationTokenSource.Token);
                }
            }
        }

        private async Task<Page> LoadPage(long precedingCheckpoint, string subscriptionId, CancellationToken cancellationToken)
        {
            List<Transaction> transactions;

            try
            {
                transactions = await Task
                    .Run(() => eventStore
                        .GetFrom((precedingCheckpoint == 0) ? (long?) null : precedingCheckpoint)
                        .Take(maxPageSize)
                        .ToList(), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!(exception is TaskCanceledException))
            {
                logError(() => $"Failed loading transactions preceding checkpoint {precedingCheckpoint} from event store: " +
                               exception);

                throw;
            }

            if (transactions.Count > 0)
            {
                logDebug(() =>
                    $"Loaded from Event Store: (subscription: {subscriptionId}, precedingCheckpoint: {precedingCheckpoint}, transactions: {transactions.Count}, range: {transactions.First().Checkpoint}-{transactions.Last().Checkpoint}.");

                if (transactions.First().Checkpoint <= precedingCheckpoint)
                {
                    throw new InvalidOperationException(
                        $"The event store returned a transaction with checkpoint {transactions.First().Checkpoint} that is supposed to be higher than the requested {precedingCheckpoint}");
                }
            }
            else
            {
                logDebug(() =>
                    $"No Transactions: (subscriptionId: {subscriptionId}, precedingCheckpoint: {precedingCheckpoint})");
            }

            return new Page(precedingCheckpoint, transactions);
        }

        private void CachePage(Page page, string subscriptionId)
        {
            if (IsCachingEnabled)
            {
                IReadOnlyList<Transaction> transactions = page.Transactions;

                // Add to cache in reverse order to prevent other projectors seeing the first transactions on the cache before the entire page is there. 
                for (int index = transactions.Count - 1; index > 0; index--)
                {
                    cachedTransactionsByPrecedingCheckpoint.Set(transactions[index - 1].Checkpoint, transactions[index]);
                }

                cachedTransactionsByPrecedingCheckpoint.Set(page.PrecedingCheckpoint, transactions[0]);

                logDebug(() =>
                    $"Cached: (subscription: {subscriptionId}, precedingCheckpoint: {page.PrecedingCheckpoint}, transactions: {transactions.Count}, range: {transactions.First().Checkpoint}-{transactions.Last().Checkpoint}.");
            }
        }

        private bool IsCachingEnabled
        {
            get { return (cachedTransactionsByPrecedingCheckpoint != null); }
        }

        public void Dispose()
        {
            lock (subscriptionLock)
            {
                if (!disposingCancellationTokenSource.IsCancellationRequested)
                {
                    disposingCancellationTokenSource.Cancel();

                    foreach (Subscription subscription in subscriptions)
                    {
                        subscription.Complete();
                    }

                    // New loading tasks are no longer started at this point.
                    // After the current loading task is finished, the event store is no longer used and can be disposed.
                    Task taskToWaitFor = Volatile.Read(ref loader);

                    try
                    {
                        taskToWaitFor?.Wait();
                    }
                    catch (AggregateException)
                    {
                        // Ignore.
                    }

                    // ReSharper disable once SuspiciousTypeConversion.Global
                    (eventStore as IDisposable)?.Dispose();
                }
            }
        }
    }

    internal class LoadRequest
    {
        private readonly TaskCompletionSource<Page> completionSource;

        public LoadRequest()
        {
            completionSource = new TaskCompletionSource<Page>();
        }

        public string SubscriptionId { get; set; }

        public Task<Page> Page => completionSource.Task;

        public long PrecedingCheckpoint { get; set; }

        public CancellationToken CancellationToken { get; set; }

        public void SetResult(Page page)
        {
            completionSource.SetResult(page);
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