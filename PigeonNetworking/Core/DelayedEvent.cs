namespace PigeonNetworking.Core
{
    public abstract class DelayedEvent
    {
        public abstract void Invoke();
        public long ScheduledTime;
    }
}

