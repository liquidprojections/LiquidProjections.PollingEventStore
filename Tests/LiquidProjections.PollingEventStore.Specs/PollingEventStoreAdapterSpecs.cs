﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using Chill;
using FakeItEasy;
using FluentAssertions;
using FluentAssertions.Extensions;
using LiquidProjections.Abstractions;
using Xunit;

namespace LiquidProjections.PollingEventStore.Specs
{
    namespace PollingEventStoreAdapterSpecs
    {
        public class When_the_persistency_engine_is_temporarily_unavailable : GivenSubject<CreateSubscription>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private Transaction actualTransaction;

            public When_the_persistency_engine_is_temporarily_unavailable()
            {
                Given(() =>
                {
                    UseThe(new TransactionBuilder().WithCheckpoint(123).Build());

                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Returns(new[] {The<Transaction>()});
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Throws(new ApplicationException()).Once();

                    var adapter = new PollingEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow);

                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    Subject(null, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            actualTransaction = transactions.First();
                            return Task.FromResult(0);
                        }
                    }, "someId");
                });
            }

            [Fact]
            public async Task Then_it_should_recover_automatically_after_its_polling_interval_expires()
            {
                do
                {
                    // REFACTOR: probably better to track polling invocations
                    await Task.Delay(pollingInterval);
                }
                while (actualTransaction == null);

                actualTransaction.Id.Should().Be(The<Transaction>().Id);
            }
        }

        public class When_a_commit_is_persisted : GivenSubject<CreateSubscription>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private readonly TaskCompletionSource<Transaction> transactionHandledSource = new TaskCompletionSource<Transaction>();

            public When_a_commit_is_persisted()
            {
                Given(() =>
                {
                    UseThe(new TransactionBuilder().WithCheckpoint(123).Build());

                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Returns(new[] {The<Transaction>()});

                    var adapter = new PollingEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow);

                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    Subject(null, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            transactionHandledSource.SetResult(transactions.First());

                            return Task.FromResult(0);
                        }
                    }, "someId");
                });
            }

            [Fact]
            public async Task Then_it_should_convert_the_commit_details_to_a_transaction()
            {
                Transaction actualTransaction = await transactionHandledSource.Task.TimeoutAfter(30.Seconds());

                var transaction = The<Transaction>();
                actualTransaction.Id.Should().Be(transaction.Id);
                actualTransaction.Checkpoint.Should().Be(transaction.Checkpoint);
                actualTransaction.TimeStampUtc.Should().Be(transaction.TimeStampUtc);
                actualTransaction.StreamId.Should().Be(transaction.StreamId);

                actualTransaction.Events.Should()
                    .BeEquivalentTo(transaction.Events, options => options.ExcludingMissingMembers());
            }
        }

        public class When_requesting_a_subscription_beyond_the_highest_available_checkpoint : GivenSubject<CreateSubscription>
        {
            private readonly TaskCompletionSource<bool> noSuchCheckpointRaised = new TaskCompletionSource<bool>();

            public When_requesting_a_subscription_beyond_the_highest_available_checkpoint()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(999)).Returns(new Transaction[0]);

                    var adapter = new PollingEventStoreAdapter(eventStore, 11, 1.Seconds(), 100, () => DateTime.UtcNow);

                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    Subject(1000, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            throw new InvalidOperationException("The adapter should not provide any events.");
                        },
                        NoSuchCheckpoint = info =>
                        {
                            noSuchCheckpointRaised.SetResult(true);

                            return Task.FromResult(0);
                        }
                    }, "myIde");
                });
            }

            [Fact]
            public async Task Then_it_should_notify_the_subscriber_about_the_non_existing_checkpoint()
            {
                await noSuchCheckpointRaised.Task.TimeoutAfter(30.Seconds());
            }
        }

        public class When_there_are_no_more_commits : GivenSubject<CreateSubscription, IDisposable>
        {
            private readonly BlockingCollection<PollingCall> pollingTimeStamps = new BlockingCollection<PollingCall>();
            private readonly TaskCompletionSource<bool> pollingCompleted = new TaskCompletionSource<bool>();
            private int subscriptionCheckpoint = 1;
            private readonly TimeSpan pollingInterval = 5.Seconds();

            public When_there_are_no_more_commits()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();

                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily<IEnumerable<Transaction>, long?>(
                        checkpoint =>
                        {
                            pollingTimeStamps.Add(new PollingCall(checkpoint, DateTime.UtcNow));
                            if (pollingTimeStamps.Count == 4)
                            {
                                pollingCompleted.SetResult(true);
                            }

                            long checkPoint = checkpoint ?? 0;
                            long offsetToDetectAheadSubscriber = 1;

                            if (checkPoint <= (subscriptionCheckpoint - offsetToDetectAheadSubscriber))
                            {
                                return new TransactionBuilder().WithCheckpoint(checkPoint + 1).BuildAsEnumerable();
                            }
                            else
                            {
                                return new Transaction[0];
                            }
                        });

                    var adapter = new PollingEventStoreAdapter(eventStore, 0, pollingInterval, 100, () => DateTime.UtcNow);
                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    return Subject(subscriptionCheckpoint, new Subscriber
                    {
                        HandleTransactions = (transactions, info) => Task.FromResult(0)
                    }, "someId");
                });
            }

            [Fact]
            public async Task Then_it_should_wait_for_the_polling_interval_before_polling_again()
            {
                await pollingCompleted.Task.TimeoutAfter(30.Seconds());

                Result.Dispose();

                PollingCall lastButOneCall = pollingTimeStamps.Reverse().Skip(1).Take(1).Single();
                PollingCall lastCall = pollingTimeStamps.Last();

                lastCall.TimeStampUtc.Should().BeAtLeast(pollingInterval).After(lastButOneCall.TimeStampUtc);
                lastCall.Checkpoint.Should().Be(lastButOneCall.Checkpoint);
            }

            private class PollingCall
            {
                public PollingCall(long? checkpoint, DateTime timeStampUtc)
                {
                    Checkpoint = checkpoint;
                    TimeStampUtc = timeStampUtc;
                }
        
                public long? Checkpoint { get; set; }
                public DateTime TimeStampUtc { get; set; }
            }
        }

        public class When_a_commit_is_already_projected : GivenSubject<CreateSubscription>
        {
            private readonly TaskCompletionSource<Transaction> transactionHandledSource = new TaskCompletionSource<Transaction>();

            public When_a_commit_is_already_projected()
            {
                Given(() =>
                {
                    Transaction projectedCommit = new TransactionBuilder().WithCheckpoint(123).Build();
                    Transaction unprojectedCommit = new TransactionBuilder().WithCheckpoint(124).Build();

                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Returns(new[] {projectedCommit, unprojectedCommit});
                    A.CallTo(() => eventStore.GetFrom(123)).Returns(new[] {unprojectedCommit});

                    var adapter = new PollingEventStoreAdapter(eventStore, 11, 1.Seconds(), 100, () => DateTime.UtcNow);
                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    Subject(123, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            transactionHandledSource.SetResult(transactions.First());

                            return Task.FromResult(0);
                        }
                    }, "someId");
                });
            }

            [Fact]
            public async Task Then_it_should_convert_the_unprojected_commit_details_to_a_transaction()
            {
                Transaction actualTransaction = await transactionHandledSource.Task.TimeoutAfter(30.Seconds());

                actualTransaction.Checkpoint.Should().Be(124);
            }
        }

        public class When_disposing_the_adapter : GivenSubject<PollingEventStoreAdapter>
        {
            private readonly DateTime utcNow = DateTime.UtcNow;

            public When_disposing_the_adapter()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Returns(new Transaction[0]);

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, 11, 500.Milliseconds(), 100, () => utcNow));

                    Subject.Subscribe(null, new Subscriber
                    {
                        HandleTransactions = (transactions, info) => Task.FromResult(0)
                    }, "someId");
                });

                When(() => Subject.Dispose(), deferredExecution: true);
            }

            [Fact]
            public void Then_it_should_stop()
            {
                if (!Task.Run(() => WhenAction.Should().NotThrow()).Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The adapter has not stopped in 10 seconds.");
                }
            }
        }

        public class When_disposing_subscription : GivenSubject<PollingEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private readonly DateTime utcNow = DateTime.UtcNow;
            private IDisposable subscription;

            public When_disposing_subscription()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).Returns(new Transaction[0]);

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => utcNow));

                    subscription = Subject.Subscribe(null, new Subscriber
                    {
                        HandleTransactions = (transactions, info) => Task.FromResult(0)
                    }, "someId");
                });

                When(() => subscription.Dispose(), deferredExecution: true);
            }

            [Fact]
            public void Then_it_should_stop()
            {
                if (!Task.Run(() => WhenAction.Should().NotThrow()).Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The subscription has not stopped in 10 seconds.");
                }
            }
        }

        public class
            When_a_subscription_starts_after_zero_checkpoint_and_another_subscription_starts_after_null_checkpoint_while_the_first_subscription_is_loading_the_first_page_from_the_event_store :
                GivenSubject<CreateSubscription>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private readonly ManualResetEventSlim aSubscriptionStartedLoading = new ManualResetEventSlim();
            private readonly ManualResetEventSlim secondSubscriptionCreated = new ManualResetEventSlim();
            private readonly ManualResetEventSlim secondSubscriptionReceivedTheTransaction = new ManualResetEventSlim();

            public
                When_a_subscription_starts_after_zero_checkpoint_and_another_subscription_starts_after_null_checkpoint_while_the_first_subscription_is_loading_the_first_page_from_the_event_store()
            {
                Given(() =>
                {
                    IPassiveEventStore eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily(call =>
                    {
                        long checkpoint = call.GetArgument<long?>(0) ?? 0;

                        aSubscriptionStartedLoading.Set();

                        if (!secondSubscriptionCreated.Wait(TimeSpan.FromSeconds(10)))
                        {
                            throw new InvalidOperationException("The second subscription has not been created in 10 seconds.");
                        }

                        // Give the second subscription enough time to access the cache.
                        Thread.Sleep(TimeSpan.FromSeconds(1));

                        return checkpoint > 0
                            ? new Transaction[0]
                            : new[] {new TransactionBuilder().WithCheckpoint(1).Build()};
                    });

                    var adapter = new PollingEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow);
                    WithSubject(_ => adapter.Subscribe);
                });

                When(() =>
                {
                    Subject(0, new Subscriber
                    {
                        HandleTransactions = (transactions, info) => Task.FromResult(0)
                    }, "firstId");

                    if (!aSubscriptionStartedLoading.Wait(TimeSpan.FromSeconds(10)))
                    {
                        throw new InvalidOperationException("The first subscription has not started loading in 10 seconds.");
                    }

                    Subject(null, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            secondSubscriptionReceivedTheTransaction.Set();
                            return Task.FromResult(0);
                        }
                    }, "secondId");

                    secondSubscriptionCreated.Set();
                });
            }

            [Fact]
            public void Then_the_second_subscription_should_not_hang()
            {
                if (!secondSubscriptionReceivedTheTransaction.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The second subscription has not got the transaction in 10 seconds.");
                }
            }
        }

        public class When_the_subscriber_cancels_the_subscription_from_inside_its_transaction_handler :
            GivenSubject<PollingEventStoreAdapter>
        {
            private readonly ManualResetEventSlim disposed = new ManualResetEventSlim();

            public When_the_subscriber_cancels_the_subscription_from_inside_its_transaction_handler()
            {
                Given(() =>
                {
                    UseThe(A.Fake<IPassiveEventStore>());
                    A.CallTo(() => The<IPassiveEventStore>().GetFrom(A<long?>.Ignored)).Returns(new[]
                    {
                        new TransactionBuilder().WithCheckpoint(123).Build()
                    });

                    WithSubject(_ => new PollingEventStoreAdapter(The<IPassiveEventStore>(), 11, 500.Milliseconds(), 100,
                        () => DateTime.UtcNow));
                });

                When(() => Subject.Subscribe(0,
                    new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            info.Subscription.Dispose();
                            disposed.Set();
                            return Task.FromResult(0);
                        }
                    },
                    "someId"));
            }

            [Fact]
            public void Then_it_should_cancel_the_subscription_asynchronously()
            {
                if (!disposed.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The subscription was not disposed in 10 seconds.");
                }
            }
        }

        public class When_the_transaction_handler_has_delay_which_uses_the_cancellation_token_and_the_subscription_is_cancelled :
            GivenSubject<PollingEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private readonly DateTime utcNow = DateTime.UtcNow;
            private readonly ManualResetEventSlim transactionHandlerStarted = new ManualResetEventSlim();
            private readonly ManualResetEventSlim transactionHandlerCancelled = new ManualResetEventSlim();
            private IDisposable subscription;

            public When_the_transaction_handler_has_delay_which_uses_the_cancellation_token_and_the_subscription_is_cancelled()
            {
                Given(() =>
                {
                    UseThe(new TransactionBuilder().WithCheckpoint(123).Build());

                    UseThe(A.Fake<IPassiveEventStore>());
                    A.CallTo(() => The<IPassiveEventStore>().GetFrom(A<long?>.Ignored)).Returns(new[] {The<Transaction>()});

                    WithSubject(_ => new PollingEventStoreAdapter(The<IPassiveEventStore>(), 11, pollingInterval, 100,
                        () => utcNow));

                    subscription = Subject.Subscribe(null,
                        new Subscriber
                        {
                            HandleTransactions = async (transactions, info) =>
                            {
                                transactionHandlerStarted.Set();

                                try
                                {
                                    await Task.Delay(TimeSpan.FromDays(1), info.CancellationToken.Value);
                                }
                                catch (OperationCanceledException)
                                {
                                    transactionHandlerCancelled.Set();
                                }
                            }
                        },
                        "someId");
                });

                When(() =>
                {
                    transactionHandlerStarted.Wait();
                    subscription.Dispose();
                });
            }

            [Fact]
            public void Then_it_should_cancel_the_transaction_handler()
            {
                if (!transactionHandlerCancelled.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The transaction handler was not cancelled in 10 seconds.");
                }
            }
        }

        public class When_a_cache_hitting_request_does_not_align_with_the_cached_page_size :
            GivenSubject<PollingEventStoreAdapter>
        {
            private int pageSize = 1000;
            private Transaction[] firstPage;
            private Transaction[] secondPage;
            private readonly ConcurrentQueue<ExpectedCall> expectedCalls = new ConcurrentQueue<ExpectedCall>(); 

            public When_a_cache_hitting_request_does_not_align_with_the_cached_page_size()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily(call =>
                    {
                        if (expectedCalls.TryDequeue(out ExpectedCall expectedCall))
                        {
                            return new TransactionBatchBuilder().FollowingCheckpoint(expectedCall.PrecedingCheckpoint).WithPageSize(expectedCall.PageSize).Build();
                        }

                        return new Transaction[0];
                    });

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, cacheSize: pageSize * 2,
                        pollInterval: 500.Milliseconds(), maxPageSize: pageSize, getUtcNow: () => DateTime.Now));
                });

                When(async () =>
                {
                    // Return an incomplete page to prevent the adapter from pre-loading the next page.
                    expectedCalls.Enqueue(new ExpectedCall { PrecedingCheckpoint = 0, PageSize = pageSize - 1 });

                    IReadOnlyList<Transaction> firstSubscribersPage = null; 
                    
                    // Let the first subscription load that incomplete page into the cache.
                    ISubscription subscription = Subject.Subscribe(0, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            firstSubscribersPage = transactions;

                            info.Subscription.Dispose();

                            return Task.FromResult(0);
                        }
                    }, "first");

                    await subscription.Disposed;

                    // Expect a request for the page following the incomplete page.
                    expectedCalls.Enqueue(new ExpectedCall {PrecedingCheckpoint = firstSubscribersPage.Last().Checkpoint, PageSize = pageSize});

                    // Let the second subscription start half-way the typical page size.
                    subscription = Subject.Subscribe(pageSize / 2, new Subscriber
                    {
                        HandleTransactions = (transactions, info) =>
                        {
                            if (firstPage == null)
                            {
                                firstPage = transactions.ToArray();
                            }
                            else if (secondPage == null)
                            {
                                secondPage = transactions.ToArray();
                                info.Subscription.Dispose();
                            }

                            return Task.FromResult(0);
                        }
                    }, "second");

                    await subscription.Disposed;
                });
            }

            [Fact]
            public void Then_the_first_page_should_contain_the_cached_transactions_only()
            {
                firstPage.Should().HaveCount((pageSize / 2) - 1);
            }

            [Fact]
            public void And_the_second_page_should_contain_a_full_set_again()
            {
                secondPage.Should().HaveCount(pageSize);
            }
            
            private class ExpectedCall
            {
                public long PrecedingCheckpoint { get; set; }

                public int PageSize { get; set; }
            }
        }
        
        public class When_a_full_page_is_loaded_and_caching_is_enabled : GivenSubject<PollingEventStoreAdapter>
        {
            private int pageSize = 1000;
            private readonly TaskCompletionSource<bool> firstPageIsLoadedInCache = new TaskCompletionSource<bool>();
            private readonly List<long> requestedCheckpoints = new List<long>();

            public When_a_full_page_is_loaded_and_caching_is_enabled()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily(call =>
                    {
                        long precedingCheckpoint = call.GetArgument<long?>(0) ?? 0;

                        requestedCheckpoints.Add(precedingCheckpoint);

                        return Enumerable.Range((int) precedingCheckpoint + 1, pageSize)
                            .Select(cp => new TransactionBuilder().WithCheckpoint(cp).Build()).ToArray();
                    });

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, cacheSize: pageSize * 2,
                        pollInterval: 500.Milliseconds(), maxPageSize: pageSize, getUtcNow: () => DateTime.Now));

                });

                When(async () =>
                {
                    ISubscription subscription = Subject.Subscribe(0, new Subscriber
                    {
                        HandleTransactions = (_, info) =>
                        {
                            info.Subscription.Dispose();
                            
                            firstPageIsLoadedInCache.SetResult(true);

                            return Task.FromResult(0);
                        }
                    }, "id");

                    await subscription.Disposed;
                });
            }

            [Fact]
            public async Task Then_it_should_preload_the_next_page()
            {
                await firstPageIsLoadedInCache.Task;

                requestedCheckpoints.Should().BeEquivalentTo(new[] {0, 1000});
            }
        }

        public class When_a_partial_page_is_loaded_and_caching_is_enabled : GivenSubject<PollingEventStoreAdapter>
        {
            private int pageSize = 1000;
            private readonly TaskCompletionSource<bool> firstPageIsLoadedInCache = new TaskCompletionSource<bool>();
            private readonly List<long> requestedCheckpoints = new List<long>();

            public When_a_partial_page_is_loaded_and_caching_is_enabled()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily(call =>
                    {
                        long precedingCheckpoint = call.GetArgument<long?>(0) ?? 0;

                        requestedCheckpoints.Add(precedingCheckpoint);

                        return Enumerable.Range((int) precedingCheckpoint + 1, pageSize / 2)
                            .Select(cp => new TransactionBuilder().WithCheckpoint(cp).Build()).ToArray();
                    });

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, cacheSize: pageSize * 2,
                        pollInterval: 500.Milliseconds(), maxPageSize: pageSize, getUtcNow: () => DateTime.Now));

                });

                When(async () =>
                {
                    var subscription = Subject.Subscribe(0, new Subscriber
                    {
                        HandleTransactions = (_, info) =>
                        {
                            info.Subscription.Dispose();
                            
                            firstPageIsLoadedInCache.SetResult(true);

                            return Task.FromResult(0);
                        }
                    }, "id");
                    
                    await subscription.Disposed;
                });
            }

            [Fact]
            public async Task Then_it_should_not_preload_the_next_page()
            {
                await firstPageIsLoadedInCache.Task;

                requestedCheckpoints.Should().BeEquivalentTo(new[] {0});
            }
        }
        
        public class When_a_full_page_is_loaded_but_caching_is_disabled : GivenSubject<PollingEventStoreAdapter>
        {
            private int pageSize = 1000;
            private readonly TaskCompletionSource<bool> firstPageIsLoadedInCache = new TaskCompletionSource<bool>();
            private readonly List<long> requestedCheckpoints = new List<long>();

            public When_a_full_page_is_loaded_but_caching_is_disabled()
            {
                Given(() =>
                {
                    var eventStore = A.Fake<IPassiveEventStore>();
                    A.CallTo(() => eventStore.GetFrom(A<long?>.Ignored)).ReturnsLazily(call =>
                    {
                        long precedingCheckpoint = call.GetArgument<long?>(0) ?? 0;

                        requestedCheckpoints.Add(precedingCheckpoint);

                        return Enumerable.Range((int) precedingCheckpoint + 1, pageSize)
                            .Select(cp => new TransactionBuilder().WithCheckpoint(cp).Build()).ToArray();
                    });

                    WithSubject(_ => new PollingEventStoreAdapter(eventStore, cacheSize: 0,
                        pollInterval: 500.Milliseconds(), maxPageSize: pageSize, getUtcNow: () => DateTime.Now));

                });

                When(async () =>
                {
                    var subscription = Subject.Subscribe(0, new Subscriber
                    {
                        HandleTransactions = (_, info) =>
                        {
                            info.Subscription.Dispose();
                            
                            firstPageIsLoadedInCache.SetResult(true);

                            return Task.FromResult(0);
                        }
                    }, "id");

                    await subscription.Disposed;
                });
            }

            [Fact]
            public async Task Then_it_should_not_preload_the_next_page()
            {
                await firstPageIsLoadedInCache.Task;

                requestedCheckpoints.Should().BeEquivalentTo(new[] {0});
            }
        }

    }
}