using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using PigeonNetworking.Transports.UDP;

namespace PigeonNetworking.Core
{
    public class Peer
    {
        public IPEndPoint IpEndPoint;
        public EndpointKey EndpointKey;
        public ushort Id;
        public bool Accepted;

        internal bool IsInvalid = false; // true if already disconnected and removed from client registry
    
        /// <summary>
        /// Get round trip time based on heartbeats in milliseconds
        /// Might not get set for all transports, unless heartbeats are enabled
        /// </summary>
        public int HeartbeatRtt => _heartbeatRTT;

        /// <summary>
        /// Get round trip time of the transport in milliseconds
        /// </summary>
        public int RTT => _transportInternalRTT;

        internal long LastSentHeartbeat { get; set; }
        internal long LastReceivedHeartbeat { get; set; }
        internal ushort LastHeartbeatId { get; set; }
        internal int SentHeartbeats { get; set; } // how many heartbeats have we sent in total

        /// <summary>
        /// round trip time in milliseconds based on heartbeats round trip
        /// </summary>
        private int _heartbeatRTT = 222;

        /// <summary>
        /// round trip time in milliseconds custom for transport
        /// </summary>
        private int _transportInternalRTT = 222;

        internal void SetHeartbeatRTT(int rtt)
        {
            _heartbeatRTT = rtt;
        }
        internal void SetTransportRTT(int rtt)
        {
            _transportInternalRTT = rtt;
        }
        
        internal byte[] NagleMessageBuffer = new byte[Message.MaxSize];
        internal int NagleMessageWritePosition = 0;
        internal ushort NagledMessages = 0;
        
        internal void WriteMessageToNagleBuffer(Message message)
        {
            if (NagleMessageWritePosition == 0)
            {
                ResetNagleMessageBuffer();
            }
            
            try
            {
                BinaryPrimitives.WriteUInt16LittleEndian(NagleMessageBuffer.AsSpan(NagleMessageWritePosition), (ushort)message.BytesInUse);
                NagleMessageWritePosition += 2;
                Array.Copy(message.GetBuffer(), 0, NagleMessageBuffer, NagleMessageWritePosition, message.BytesInUse);
                NagleMessageWritePosition += message.BytesInUse;
                NagledMessages++;
            }
            catch (Exception e)
            {
                PnLog.LogException("Failed to write message to nagle buffer. Exception: ", e);
            }
        }
        internal void ResetNagleMessageBuffer()
        {
            NagledMessages = 0;
            NagleMessageWritePosition = 3;
            NagleMessageBuffer[0] = (byte)EHeader.NagledMessageBatch;
        }

        internal Queue<ushort> QueuedAcks = new Queue<ushort>();
        internal Queue<ushort> RecentlyReceivedReliableMessageIDs = new();
        internal ConcurrentDictionary<ushort, PendingReliableMessage> PendingReliableMessages = new();


        internal byte LastSentReliableByteStreamId = 1;
        internal Dictionary<byte, SortedDictionary<ushort, byte[]>> receivedLargeByteStreamChunks = new Dictionary<byte, SortedDictionary<ushort,byte[]>>();


        internal void Clear()
        {
            QueuedAcks.Clear();
            RecentlyReceivedReliableMessageIDs.Clear();
            receivedLargeByteStreamChunks.Clear();
            foreach (var prm in PendingReliableMessages)
            {
                prm.Value.AcknowledgeAndRelease();
            }
            
            PendingReliableMessages.Clear();
        }

    }
}

