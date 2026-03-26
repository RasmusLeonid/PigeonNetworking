using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace PigeonNetworking.Statistics
{
    
    /// <summary>
    /// To use this, call Tick every frame from the main thread and use the getter methods, to get the available stats that are tracked.
    /// Keep in mind not all transports track all stats. Steam Transport handles stuff like resending and nagle by itself, so only sent and received packets are tracked.
    /// Whilst in the UDP transport everything is Handled by PigeonNetworking, so we fully track everything.
    /// </summary>
    public static class PnStatistics
    {
        
        private const int WindowSize = 10;
        private static readonly int[] sentPacketsPerSecond = new int[WindowSize];
        private static readonly int[] receivedPacketsPerSecond = new int[WindowSize];
        private static readonly int[] ackBatchesSentPerSecond = new int[WindowSize];
        private static readonly int[] ackBatchesReceivedPerSecond = new int[WindowSize];
        private static readonly int[] acksSentPerSecond = new int[WindowSize];
        private static readonly int[] acksReceivedPerSecond = new int[WindowSize];
        
        private static readonly int[] resentMessagesPerSecond = new int[WindowSize];
        private static readonly int[] reSentMessagesReceivedPerSecond = new int[WindowSize];
        
        
        private static int currentSecondIndex = 0;
        private static int currentSecondPacketsSent = 0;
        private static int currentSecondPacketsReceived = 0;
        private static int currentSecondAckBatchesSent = 0;
        private static int currentSecondAckBatchesReceived = 0;
        private static int currentSecondAcksSent = 0;
        private static int currentSecondAcksReceived = 0;
        private static int currentSecondResentMessages = 0;
        private static int currentSecondReSentMessagesReceived = 0;
        
        
        
        private static double lastSecondTime;
        
        
        /// <summary>
        /// Call this at least once per second. Every frame is fine too.
        /// </summary>
        public static void Tick()
        {
            long now = Stopwatch.GetTimestamp();
            if (now - lastSecondTime >= 1.0)
            {
                sentPacketsPerSecond[currentSecondIndex] = currentSecondPacketsSent;
                receivedPacketsPerSecond[currentSecondIndex] = currentSecondPacketsReceived;
                resentMessagesPerSecond[currentSecondIndex] = currentSecondResentMessages;
                reSentMessagesReceivedPerSecond[currentSecondIndex] = currentSecondReSentMessagesReceived;
                
                ackBatchesReceivedPerSecond[currentSecondIndex] = currentSecondAckBatchesReceived;
                ackBatchesSentPerSecond[currentSecondIndex] = currentSecondAckBatchesSent;
                
                acksReceivedPerSecond[currentSecondIndex] = currentSecondAcksReceived;
                acksSentPerSecond[currentSecondIndex] = currentSecondAcksSent;
                
                
                currentSecondIndex = (currentSecondIndex + 1) % WindowSize;

                
                currentSecondResentMessages = 0;
                currentSecondReSentMessagesReceived = 0;
                currentSecondPacketsSent = 0;
                currentSecondPacketsReceived = 0;
                currentSecondAckBatchesSent = 0;
                currentSecondAckBatchesReceived = 0;
                currentSecondAcksSent = 0;
                currentSecondAcksReceived = 0;
                lastSecondTime = now;
            }
        }

        
        /// <summary>
        /// Gets the amount of datagrams sent during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetPacketsSentLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return sentPacketsPerSecond[index];
        }

        /// <summary>
        /// Gets the average amount of sent datagrams per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetPacketsSentAverage()
        {
            return sentPacketsPerSecond.Sum() / (float)WindowSize;
        }
        
        /// <summary>
        /// Gets the amount of datagrams received during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetPacketsReceivedLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return receivedPacketsPerSecond[index];
        }

        /// <summary>
        /// Gets the average amount of datagrams received per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetPacketsReceivedAverage()
        {
            return receivedPacketsPerSecond.Sum() / (float)WindowSize;
        }

        /// <summary>
        /// Get the amount of messages that got resent during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetResentMessagesLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return resentMessagesPerSecond[index];
        }

        /// <summary>
        /// Get the average messages resent per second, during the last 10 seconds.
        /// </summary>
        /// <returns></returns>
        public static float GetResentMessagesAverage()
        {
            return resentMessagesPerSecond.Sum() / (float)WindowSize;
        }

        /// <summary>
        /// Get the amount of duplicate messages we received during the last second
        /// duplicate messages are filtered out internally. Messages usually get resent by the remote, if we fail to send an ack in time  or the ack gets los
        /// </summary>
        /// <returns></returns>
        public static int GetReSentMessagesReceivedLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return reSentMessagesReceivedPerSecond[index];
        }

        /// <summary>
        /// Get the average amout of duplicate messages we receive per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetReSentMessagesReceivedAverage()
        {
            return reSentMessagesReceivedPerSecond.Sum() / (float)WindowSize;
        }

        
        /// <summary>
        /// Get the amount of Ack batches we sent, during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetAckBatchesSentLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return ackBatchesSentPerSecond[index];
        }

        /// <summary>
        /// Get the average amount of Ack batches sent per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetAckBatchesSentAverage()
        {
            return ackBatchesSentPerSecond.Sum() / (float)WindowSize;
        }
        
        
        /// <summary>
        /// Get the ack batches received during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetAckBatchesReceivedLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return ackBatchesReceivedPerSecond[index];
        }
        
        /// <summary>
        /// Get the average amount of ack batches received per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetAckBatchesReceivedAverage()
        {
            return ackBatchesReceivedPerSecond.Sum() / (float)WindowSize;
        }

        
        /// <summary>
        /// Get the amount of Acks received during the last second
        /// </summary>
        /// <returns></returns>
        public static int GetAcksReceivedLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return acksReceivedPerSecond[index];
        }

        /// <summary>
        /// Get the average amount of acks received per second, during the last 10 seconds
        /// </summary>
        /// <returns></returns>
        public static float GetAcksReceivedAverage()
        {
            return acksReceivedPerSecond.Sum() / (float)WindowSize;
        }

        /// <summary>
        /// Get Acks sent during the last second.
        /// </summary>
        /// <returns></returns>
        public static int GetAcksSentLastSecond()
        {
            int index = (currentSecondIndex - 1 + WindowSize) % WindowSize;
            return acksSentPerSecond[index];
        }

        /// <summary>
        /// Get the average amount of acks sent per second, during the last 10 seconds.
        /// </summary>
        /// <returns></returns>
        public static float GetAcksSentAverage()
        {
            return acksSentPerSecond.Sum() / (float)WindowSize;
        }
    
        
        internal static void MessageSent(EChannel channel)
        {
        
        }

        internal static void IOSent()
        {
            Interlocked.Increment(ref currentSecondPacketsSent);
        }
        internal static void IOReceived()
        {
            Interlocked.Increment(ref currentSecondPacketsReceived);
        }
    
        internal static void MessageReceived(EChannel channel)
        {
            
        }

        internal static void AcksReceived(int batchCound)
        {
            Interlocked.Increment(ref currentSecondAckBatchesReceived);
            Interlocked.Add(ref currentSecondAcksReceived, batchCound);
        }

        internal static void AcksSent(int batchCound)
        {
            Interlocked.Increment(ref currentSecondAckBatchesSent);
            Interlocked.Add(ref currentSecondAcksSent, batchCound);
        }

        internal static void MessageResent()
        {
            Interlocked.Increment(ref currentSecondResentMessages);
        }
        internal static void ReSentMessageReceived()
        {
            Interlocked.Increment(ref currentSecondReSentMessagesReceived);
        }
    }
}

