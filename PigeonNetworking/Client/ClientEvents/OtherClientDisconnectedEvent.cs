using System;

namespace PigeonNetworking.Client.ClientEvents
{
    public class OtherClientDisconnectedEvent : EventArgs
    {
        public ushort ClientId { get; set; }
        public OtherClientDisconnectedEvent(ushort clientId) : base()
        {
            ClientId = clientId;
        }
    } 
}

