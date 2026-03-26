using System;

namespace PigeonNetworking.Client.ClientEvents
{
    public class OtherClientConnectedEvent : EventArgs
    {
        public ushort ClientId { get; set; }
        public OtherClientConnectedEvent(ushort clientId) : base()
        {
            ClientId = clientId;
        }
    }
}

