namespace PigeonNetworking.Core
{
    public abstract class BaseClientRegistry
    {

        internal static int totalStaticPeerCount = 1;
        
        private ushort nextClientId = 1;
        protected ushort GetNextClientId()
        {
            nextClientId++;
            return (ushort)(nextClientId - 1);
        }

        public static void TrimPools()
        {
            Message.Trim();
            PendingReliableMessage.Trim();
        }
    }
}

