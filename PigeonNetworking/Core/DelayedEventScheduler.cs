using System.Collections.Generic;
using System.Threading;
using PigeonNetworking.Client;
using PigeonNetworking.Helpers;

namespace PigeonNetworking.Core
{
    public class DelayedEventScheduler
    {
        private int myThread;
        public DelayedEventScheduler()
        {
            myThread = Thread.CurrentThread.ManagedThreadId;
        }
        
        private readonly List<DelayedEvent> pendingEvents = new();

        public DelayedEvent Schedule(DelayedEvent evt)
        {
            // if(myThread != Thread.CurrentThread.ManagedThreadId)
            //     throw new System.Exception("Cannot schedule events on different threads");
            pendingEvents.Add(evt);
            return evt;
        }
        
        public void Remove(DelayedEvent evt)
        {
            // if(myThread != Thread.CurrentThread.ManagedThreadId)
            //     throw new System.Exception("Cannot remove events on different threads");
            pendingEvents.Remove(evt);
        }

        public void Tick()
        {
            
            // if(myThread != Thread.CurrentThread.ManagedThreadId)
            //     throw new System.Exception("Cannot tick events on different threads");
            long now = ReliableResendTimer.Now;

            for (int i = pendingEvents.Count - 1; i >= 0; i--)
            {
                if (pendingEvents[i].ScheduledTime <= now)
                {
                    pendingEvents[i]?.Invoke();
                    pendingEvents.RemoveAt(i);
                }
            }

        }
    }
}

