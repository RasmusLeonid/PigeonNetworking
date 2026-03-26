using System;
using PigeonNetworking.Core;

namespace PigeonNetworking.Transports.UDP.Threading.Server
{
    public struct ReceiveToMainThreadOnServerJob 
    {
        public Message Message;
        public ushort HandlerId;
        public ushort PeerId;

        public PigeonNetworking.Server.Server Caller;
    
        public void Execute()
        {
            try
            {
                Caller.MessageHandlers[HandlerId](PeerId, Message);
            }
            catch (Exception e)
            {
                PnLog.LogException($"Failed to dispatch message to handlers. HandlerId: {HandlerId}, MessageSize: {Message.BytesInUse}. This usually happens if an unhandled exception occurs in your custom MessageHandler method. Exception: ", e);
            }
            finally
            {
                Message.Release();
            }
        }
    }
}

