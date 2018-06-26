using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections;
using LiquidProjections.Abstractions;
using LiquidProjections.PollingEventStore;

namespace SampleSubscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            
//            var adapter = new PollingEventStoreAdapter(new PassiveEventStore(), 50000, 5.Seconds(), 1000, () => DateTime.UtcNow, messageFunc => Console.WriteLine(messageFunc()));
            var adapter = new PollingEventStoreAdapter(new PassiveEventStore(), 50000, TimeSpan.FromSeconds(5), 1000, () => DateTime.UtcNow);

            int maxSubscribers = 50;
            
            for (int id = 0; id < maxSubscribers; id++)
            {
                int localId = id;
                adapter.Subscribe(0, new Subscriber
                {
                    HandleTransactions = (transactions, info) =>
                    {
                        Console.WriteLine(
                            $"{stopWatch.Elapsed}: Subscriber {info.Id} received transactions {transactions.First().Checkpoint} to {transactions.Last().Checkpoint} on thead {Thread.CurrentThread.ManagedThreadId}");

                        Thread.Sleep(new Random().Next(100, 500));

                        return Task.FromResult(0);
                    },
                    NoSuchCheckpoint = info => Task.FromResult(0)
                }, id.ToString());

                Console.WriteLine($"{stopWatch.Elapsed}: Started subscriber {localId}");
                
                Thread.Sleep(1000);
            }
            
            Console.WriteLine("Press a key to shutdown");
            Console.ReadLine();

            adapter.Dispose();
        }
    }
    
    internal class PassiveEventStore : IPassiveEventStore
    {
        private const long MaxCheckpoint = 100000;
        private int nrRequests = 0;
        
        public IEnumerable<Transaction> GetFrom(long? previousCheckpoint)
        {
            Interlocked.Increment(ref nrRequests);
            
            previousCheckpoint = previousCheckpoint ?? 0;
            if (previousCheckpoint > MaxCheckpoint)
            {
                return new List<Transaction>();
            }
            
            var transactions = new List<Transaction>();

            long firstCheckpoint = previousCheckpoint.Value + 1;
            
            for (long checkpoint = firstCheckpoint; checkpoint < firstCheckpoint + 1000 && checkpoint < MaxCheckpoint; checkpoint++)
            {
                transactions.Add(new Transaction
                {
                   Checkpoint = checkpoint,
                    Events = new List<EventEnvelope>(),
                    Headers = new Dictionary<string, object>(),
                    Id = Guid.NewGuid().ToString(),
                    StreamId = Guid.NewGuid().ToString(),
                    TimeStampUtc = DateTime.UtcNow
                });
            }
            
            Thread.Sleep(1000);
            
            Console.WriteLine($"Number of event store requests: {nrRequests}");

            return transactions;
        }
    }
}