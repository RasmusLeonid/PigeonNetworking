using System.Collections.Generic;
using PigeonNetworking.Core;

namespace PigeonNetworking.Transports.UDP
{
    public class ResendEventPool
    {
        private readonly Server.Server _server;
        
        /// <summary>
        /// Pass null, when used from a client class
        /// </summary>
        /// <param name="server"></param>
        internal ResendEventPool(Server.Server server)
        {
            _server = server;
        }

        private int GetPeerCount
        {
            get
            {
                if (_server != null)  return _server.CurrentPeers();
                return 1;
            }
        }
        
        internal Queue<ResendEvent> Queue = new Queue<ResendEvent>();
        internal ResendEvent Create(PendingReliableMessage message, long delayMs)
        {
            if (Queue.TryDequeue(out ResendEvent resendEvent))
            {
                resendEvent.OverrideNew(message, delayMs, this);
                return resendEvent;
            }
        
            return  new ResendEvent(message, delayMs, this);
        }
    
        internal void Release(ResendEvent resendEvent)
        {
            if(resendEvent._isReleased) return;
            resendEvent.Reset();
            if (Queue.Count >= GetPeerCount * 4)
            {
                TrimPool();
                return;
            }
            resendEvent._isReleased = true;
            Queue.Enqueue(resendEvent);
        }

        private void TrimPool()
        {
            while (Queue.Count > GetPeerCount * 4)
            {
                Queue.Dequeue();
            }
        }
    }
}

