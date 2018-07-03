using System;
using System.Threading.Tasks;

namespace LiquidProjections.PollingEventStore
{
    public interface ISubscription : IDisposable
    {
        /// <summary>
        /// Returns a task that completes when the subscription has been completed disposed, including any background activity. 
        /// </summary>
        Task Disposed { get; }
    }
}