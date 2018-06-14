using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using LiquidProjections;
using LiquidProjections.Abstractions;
using LiquidProjections.PollingEventStore;

namespace SampleSubscriber
{
    class Program
    {
        static void Main(string[] args)
        {
//            var adapter = new PollingEventStoreAdapter(new PassiveEventStore(), 0, 5.Seconds(), 1000, () => DateTime.UtcNow, messageFunc => Console.WriteLine(messageFunc()));
            var adapter = new PollingEventStoreAdapter(new PassiveEventStore(), 0, 5.Seconds(), 1000, () => DateTime.UtcNow);

            int maxSubscrbiers = 5;
            
            for (int id = 0; id < maxSubscrbiers; id++)
            {
                int localId = id;
                adapter.Subscribe(0, new Subscriber
                {
                    HandleTransactions = (transactions, info) =>
                    {
                        Console.WriteLine(
                            $"Subscriber {info.Id} received transactions {transactions.First().Checkpoint} to {transactions.Last().Checkpoint} on thead {Thread.CurrentThread.ManagedThreadId}");

                        Thread.Sleep(500);

                        return Task.FromResult(0);
                    },
                    NoSuchCheckpoint = info => Task.FromResult(0)
                }, id.ToString());

                Console.WriteLine($"Started subscriber {localId}");
            }
            
            Console.WriteLine("Press a key to shutdown");
            Console.ReadLine();

            adapter.Dispose();
        }
    }
    
    internal class PassiveEventStore : IPassiveEventStore
    {
        private const long MaxCheckpoint = 100000;
        
        public IEnumerable<Transaction> GetFrom(long? previousCheckpoint)
        {
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

            return transactions;
        }
    }
}