using System;

namespace PigeonNetworking.Server.ServerEvents
{
    public class ClientDisconnectedEvent : EventArgs
    {
    
        /// <summary>
        /// The Id of the peer that disconnected
        /// </summary>
        public ushort ClientId { get; private set; }
        internal ClientDisconnectedEvent(ushort clientId) : base()
        {
            ClientId = clientId;
        }
    }
}

