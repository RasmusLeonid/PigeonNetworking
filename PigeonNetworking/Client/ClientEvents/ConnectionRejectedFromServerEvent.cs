using System;
using PigeonNetworking.Core.Enums;

namespace PigeonNetworking.Client.ClientEvents
{
    public class ConnectionRejectedFromServerEvent : EventArgs
    {
        public ERejectionReason RejectionReason;
        public ConnectionRejectedFromServerEvent(ERejectionReason reason) : base()
        {
            RejectionReason = reason;
        }
    }
}

