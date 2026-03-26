using System;

namespace PigeonNetworking.Server.ServerEvents
{
    public class LargeByteStreamReceivedOnServerEvent : EventArgs
    {
        /// <summary>
        /// The byte[] we received
        /// </summary>
        public byte[] Data { get; private set;}
        
        /// <summary>
        /// The id of the Peer we received the data from
        /// </summary>
        public ushort clientId { get; private set;}

        internal LargeByteStreamReceivedOnServerEvent(byte[] data, ushort client) : base()
        {
            Data = data;
            clientId = client;
        }
    }
}

