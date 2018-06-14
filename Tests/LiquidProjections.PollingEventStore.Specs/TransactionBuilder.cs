using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;

namespace LiquidProjections.PollingEventStore.Specs
{
    public class TransactionBuilder : TestDataBuilder<Transaction>
    {
        private readonly List<EventEnvelope> events = new List<EventEnvelope>();
        private DateTime timeStamp = 29.February(2000).At(19, 45);
        private string streamId = Guid.NewGuid().ToString();
        private long checkpointToken = 1;
        private Guid commitId = Guid.NewGuid();

        protected override Transaction OnBuild()
        {
            if (!events.Any())
            {
                WithEvent(new TestEvent
                {
                    Version = 1
                });
            }

            return new Transaction
            {
                Checkpoint = checkpointToken,
                StreamId = streamId,
                Events =events,
                Headers = new Dictionary<string, object>(),
                Id = commitId.ToString(),
                TimeStampUtc = timeStamp
            };
        }

        public IEnumerable<Transaction> BuildAsEnumerable()
        {
            return new[] { Build() };
        }

        public TransactionBuilder WithEvent(Event @event)
        {
            var eventMessage = new EventEnvelopeBuilder().WithBody(@event).Build();

            events.Add(eventMessage);

            return this;
        }

        public TransactionBuilder At(DateTime timeStamp)
        {
            this.timeStamp = timeStamp;
            return this;
        }

        public TransactionBuilder On(string streamId)
        {
            this.streamId = streamId;
            return this;
        }

        public TransactionBuilder WithCheckpoint(long checkpoint)
        {
            this.checkpointToken = checkpoint;
            return this;
        }

        public TransactionBuilder WithCommitId(Guid commitId)
        {
            this.commitId = commitId;
            return this;
        }
    }

    public class TestEvent : Event
    {
    }
}