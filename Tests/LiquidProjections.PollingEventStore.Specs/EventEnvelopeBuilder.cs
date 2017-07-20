namespace LiquidProjections.PollingEventStore.Specs
{
    public class EventEnvelopeBuilder : TestDataBuilder<EventEnvelope>
    {
        private readonly EventEnvelope message = new EventEnvelope();

        protected override EventEnvelope OnBuild()
        {
            if (message.Body == null)
            {
                message.Body = new object();
            }

            return message;
        }

        public EventEnvelopeBuilder WithBody(Event body)
        {
            message.Body = body;
            return this;
        }

        public EventEnvelopeBuilder WithHeader(string key, object value)
        {
            message.Headers.Add(key, value);
            return this;
        }
    }
}