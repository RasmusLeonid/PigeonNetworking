using System;
using System.Net.Sockets;
using PigeonNetworking.Core;
using PigeonNetworking.Statistics;

namespace PigeonNetworking.Transports.UDP.Threading.Client
{
    public struct SendJob 
    {
        public Message Message;
        public UdpClientManager Caller;

        public void Execute()
        {
            try
            {
                if (Caller.AllowMessageNagle)
                {
                    Caller.AddMessageToNagleSendQueue(Message, Caller.serverPeer);
                }
                else
                {
                    Caller.Socket.Send(Message.GetBuffer(), 0, Message.BytesInUse, SocketFlags.None);
                    PnStatistics.IOSent();
                }
                
                if (Message.GetChannel() != EChannel.Unreliable)
                {
                    Caller.RegisterResending(Message, Caller.serverPeer);
                }
             
            }
            catch (Exception e)
            {
                PnLog.LogException("Failed to send message in SendJob. Exception: ", e);
            }
            finally
            {
                Message.Release();
            }
        }
    }

}

