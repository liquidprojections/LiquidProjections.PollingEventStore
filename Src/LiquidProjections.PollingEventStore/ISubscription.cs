using System;
using System.Threading.Tasks;

namespace LiquidProjections.PollingEventStore
{
#if LIQUIDPROJECTIONS_BUILD_TIME
    public
#else
    internal 
#endif
        interface ISubscription : IDisposable
    {
        /// <summary>
        /// Returns a task that completes when the subscription has been completed disposed, including any background activity. 
        /// </summary>
        Task Disposed { get; }
    }
}