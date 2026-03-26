using System.Net.Sockets;
using System.Threading;
using PigeonNetworking.Helpers;
using PigeonNetworking.Transports.UDP;

namespace PigeonNetworking.Core
{
    public abstract class BaseNetEndpoint
    {
        internal Socket Socket;
        
        internal byte Lane = 0;
        
        /// <summary>
        /// The rate in milliseconds at which the transport ticks heartbeats or checks RTT
        /// </summary>
        internal int HeartbeatRate = 500;

        
        /// <summary>
        /// Enables heartbeats on transports that don't need it, if set to true
        /// </summary>
        internal bool HeartbeatsEnabled = false;

        
        /// <summary>
        /// Delay between network thread ticks in milliseconds
        /// </summary>
        internal int TickDelay = 1;
    
        /// <summary>
        /// Time in milliseconds after which a disconnect should be triggered if no heartbeat is received
        /// </summary>
        internal int TimeoutTime = 10000;
        
        protected int AckNagleTime = 20;
    
        protected int MessageNagleTime = 5;
        
        internal bool AllowMessageNagle = true;
        
        
        
        
        /// <summary>
        /// The event scheduler used to schedule resends, heartbeats and other possible events
        /// </summary>
        protected DelayedEventScheduler _scheduler;


        /// <summary>
        /// instanced object pool for ResendEvents (Not thread safe, only use per thread instance)
        /// </summary>
        protected ResendEventPool _resendEventPool;
        
        
                
        /// <summary>
        /// Set the heartbeat interval on milliseconds
        /// Default is 500 milliseconds
        /// Note: PigeonNetworking does not sync this field to remote endpoints for now. If you change this,
        /// you must change it on the remote as well. Otherwise, it could lead to the remote detecting false timeouts. 
        /// </summary>
        /// <param name="millisecondsInterval"></param>
        public void SetHeartbeatRate(int millisecondsInterval) => HeartbeatRate = millisecondsInterval;

        
        /// <summary>
        /// Heartbeats are disabled for transports that don't need them for connection management and ping checks
        /// This enables heartbeats regardless if the transport needs them or not and will allow you to see check HeartbeatRTTs
        /// Default is false
        /// </summary>
        /// <param name="active"></param>
        public void SetHeartbeatsEnabled(bool active) => HeartbeatsEnabled = active;


        
        /// <summary>
        /// Allows you to set a timeout time, if the active transport supports custom timeout times
        /// Default for UDP Transport is 10000 milliseconds
        /// </summary>
        /// <param name="timeInMilliseconds"></param>
        public void SetTimeoutTime(int timeInMilliseconds) => TimeoutTime = timeInMilliseconds;



        
        /// <summary>
        /// Set the delay in milliseconds between ticks on the network thread. Default value is 1, which is recommended for
        /// low latency. For less time sensitive connections, using something between 15 and 100 may be more than sufficient.
        /// To prevent windows from defaulting to 15 millisecond delays between ticks, call NativeMethods.EnsureHighTickRate()
        /// and NativeMethods.ClearHighTickRate() at the end of the session.
        /// </summary>
        /// <param name="millisecondsDelay"></param>
        public void SetIoThreadTickRate(int millisecondsDelay) => TickDelay = millisecondsDelay;
        
        
        /// <summary>
        /// Set nagle timer for messages. Default value is 5 milliseconds.
        /// Nagle will hold messages up to 5 milliseconds (or whatever you set as nagle time) and batch them into one larger message, in order to reduce bandwidth usage.
        /// </summary>
        /// <param name="milliseconds"></param>
        public void SetMessageNagleTime(int milliseconds) => MessageNagleTime = milliseconds;
        
        /// <summary>
        /// Sets the nagle time for ACKs (acknowledge messages). Default value is 20 milliseconds. This will hold acks for a short duration (nagle time)
        /// in order to batch multiple acks into one message.
        /// </summary>
        /// <param name="milliseconds"></param>
        public void SetAckNagleTime(int milliseconds) => AckNagleTime = milliseconds;
        
        
        /// <summary>
        /// Enable or disable nagle for messages. Default is true. It is recommended to leave this on, and rather reduce nagle time, or manually call flush
        /// after sending very time sensitive messages. Note disabling nagle for ACKs is not supported as of now, you can only reduce nagle time to 0, which will prevent
        /// acks from being held over multiple ticks.
        /// </summary>
        /// <param name="allow"></param>
        public void SetAllowMessageNagle(bool allow) => AllowMessageNagle = allow;



        /// <summary>
        /// Send out all held messages and skip nagle timer asap
        /// </summary>
        public void FlushAllMessages()
        {
            Interlocked.Exchange(ref flushAll, 1);
        }

        /// <summary>
        /// Send out all held messages for a specific peer asap
        /// </summary>
        public  void FlushAllMessagesOnConnection(int peerId)
        {
            Interlocked.Exchange(ref flushPeerId, peerId);
        }
        
        //flush nagle messages
        protected int flushPeerId = -1;
        protected int flushAll = -1;
        


    }
}

