using System;
namespace PigeonNetworking.Client.ClientEvents
{
    public class LargeByteStreamReceivedEvent : EventArgs
    {
        public byte[] Data;

        public LargeByteStreamReceivedEvent(byte[] data) : base()
        {
            Data = data;
        }
    }
}