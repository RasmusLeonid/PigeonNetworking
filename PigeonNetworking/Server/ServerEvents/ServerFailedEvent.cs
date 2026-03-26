using System;

namespace PigeonNetworking.Server.ServerEvents
{

    public enum EServerFailedReason
    {
        Unknown,
        PortAlreadyInUse,
    }
    public class ServerFailedEvent : EventArgs
    {
        public EServerFailedReason Reason { get; private set; }
        internal ServerFailedEvent(EServerFailedReason reason) : base()
        {
            this.Reason = reason;
        }
    }
}