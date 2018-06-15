using System.Linq;

namespace LiquidProjections.PollingEventStore.Specs
{
    public class TransactionBatchBuilder : TestDataBuilder<Transaction[]>
    {
        private long firstCheckpoint;
        private int pageSize;

        protected override Transaction[] OnBuild()
        {
            return Enumerable.Range((int)firstCheckpoint, pageSize)
                .Select(cp => new TransactionBuilder().WithCheckpoint(cp).Build()).ToArray();
        }

        public TransactionBatchBuilder FollowingCheckpoint(long precedingCheckpoint)
        {
            firstCheckpoint = precedingCheckpoint + 1;
            
            return this;
        }

        public TransactionBatchBuilder WithPageSize(int pageSize)
        {
            this.pageSize = pageSize;
            
            return this;
        }
    }
}