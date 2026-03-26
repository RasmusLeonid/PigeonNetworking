using System;
using System.Net.Sockets;
using PigeonNetworking.Core;
using PigeonNetworking.Statistics;

namespace PigeonNetworking.Transports.UDP.Threading.Server
{
    public struct SendToAllExceptJob 
    {
        public Message Message;
        public ushort ExceptPeerId;
        public UdpServer Caller;
    
        public void Execute()
        {
            try
            {
                foreach (var peer in Caller.Clients())
                {
                    if (peer.Id != ExceptPeerId)
                    {
                        if (Caller.AllowMessageNagle)
                        {
                            Caller.AddMessageToNagleSendQueue(Message, peer);
                        }
                        else
                        {
                            Caller.Socket.SendTo(Message.GetBuffer(), Message.BytesInUse, SocketFlags.None, peer.IpEndPoint);
                            PnStatistics.IOSent();
                        }
                        
                        if (Message.GetChannel() != EChannel.Unreliable)
                        {
                            Caller.RegisterResending(Message, peer);
                        }
                    }
                }
            }
            catch (Exception e)
            {
               PnLog.LogException("Failed to send message in SendToAllExceptJob execution", e);
            }
            finally
            {
                 Message.Release();
            }
        }
    }
}

