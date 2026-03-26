using System;
using PigeonNetworking.Core;
using PigeonNetworking.Core.Enums;

namespace PigeonNetworking.Client.ClientEvents
{
    public class DisconnectedFromServerEvent : EventArgs
    {
        public DisconnectedFromServerEvent(EDisconnectionReason reason, Message message) : base()
        {
            Reason = reason;
            KickMessage = message;
        }
        
        public DisconnectedFromServerEvent(EDisconnectionReason reason) : base()
        {
            Reason = reason;
        }
        public EDisconnectionReason Reason { get; set; }

        
        /// <summary>
        /// manually handle if Reason == CustomKick
        /// Warning, can be null
        /// </summary>
        public Message KickMessage;

    }
}


