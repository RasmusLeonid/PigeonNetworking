using System;

namespace PigeonNetworking.Server.ServerEvents
{
    public class ServerReadyEvent : EventArgs
    {

        /// <summary>
        /// IP and port string or custom depending on active transport
        /// </summary>
        public string IpPort { get; private set; }
        
        /// <summary>
        /// SteamID to connect to, keep in mind that you have to set the correct virtual port when connecting, default is 0
        /// </summary>
        public ulong SdrId{ get; private set; }
        
        internal ServerReadyEvent(ulong sdrId)
        {
            SdrId = sdrId;
        }
    
        internal ServerReadyEvent(string ipPort)
        {
            IpPort = ipPort;
        }
    }
}

