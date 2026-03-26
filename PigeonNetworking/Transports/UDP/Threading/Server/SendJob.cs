using System;
using System.Net.Sockets;
using PigeonNetworking.Core;
using PigeonNetworking.Statistics;

namespace PigeonNetworking.Transports.UDP.Threading.Server
{
    public struct SendJob 
    {
        public Message Message;
        public ushort PeerId;
        public UdpServer Caller;

        public void Execute()
        {
            try
            {
                if (Caller._clientRegistry.TryGetById(PeerId, out var peer))
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
            catch (Exception e)
            {
                PnLog.LogException($"Failed to send message to peer {PeerId} in SendJob execution", e);
            }
            finally
            {
                  Message.Release();
            }
        }
    }
}

