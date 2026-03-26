using PigeonNetworking.Client;
using PigeonNetworking.Helpers;
using PigeonNetworking.Transports.UDP;

namespace PigeonNetworking.Core
{
    internal class ResendEvent : DelayedEvent
    {

        internal ResendEventPool pooler;
        internal PendingReliableMessage message;
        internal long scheduledAt;
        
        internal bool _isReleased = false;

        internal ResendEvent(PendingReliableMessage message, long delayMs, ResendEventPool pooler)
        {
            this.message = message;
            this.scheduledAt = ReliableResendTimer.Now;
            this.ScheduledTime = scheduledAt + ReliableResendTimer.MsToTicks(delayMs);
            this.pooler = pooler;
            this._isReleased = false;
        }

        internal void OverrideNew(PendingReliableMessage message, long delayMs, ResendEventPool pooler)
        {
            this.message = message;
            this.scheduledAt = ReliableResendTimer.Now;
            this.ScheduledTime = scheduledAt + ReliableResendTimer.MsToTicks(delayMs);
            this._isReleased = false;
            this.pooler = pooler;
        }

        public override void Invoke()
        {
            if (message != null && !message.Acknowledged)
            {
                if(message != null)
                    message.RetrySend(); // Calls SendAsync, updates LastSentTicks, etc.
            }
            
            pooler?.Release(this);
        }


        public void Reset()
        {
            scheduledAt = 0;
            message = null;
        }
    }
}

