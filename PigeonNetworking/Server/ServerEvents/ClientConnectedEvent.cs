using System;

namespace PigeonNetworking.Server.ServerEvents
{
    public class ClientConnectedEvent : EventArgs
    {
        /// <summary>
        /// The Id of the peer that connected
        /// </summary>
        public ushort ClientId { get; private set; }
        internal ClientConnectedEvent(ushort clientId) : base()
        {
            ClientId = clientId;
        }
    }
}

