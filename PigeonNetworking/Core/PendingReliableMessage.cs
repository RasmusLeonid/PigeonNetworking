using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Tasks;
using PigeonNetworking.Helpers;

namespace PigeonNetworking.Core
{

    
public class PendingReliableMessage
{
    
    /// <summary>The multiplier used to determine how long to wait before resending a pending message.</summary>
    public const float RetryTimeMultiplier = 1.2f;
    
    public static long Now => Stopwatch.GetTimestamp();
    public static double ElapsedMs(long since)
        => (Now - since) * 1000.0 / Stopwatch.Frequency;


    public byte[] GetBuffer() => data;
    public int GetBufferSize() => size;
    private readonly byte[] data;
    
    /// <summary>The length in bytes of the message.</summary>
    private int size;
    
    /// <summary>How many send attempts have been made so far.</summary>
    private byte sendAttempts;
    /// <summary>Whether the pending message has been cleared or not.</summary>
    public long LastSentTicks { get; set; }

    private bool _acknowledged = false;
    
    private bool _isReleased = false;

    public Peer GetPeer => connection;
    public bool Acknowledged => _acknowledged;

    private Peer connection;

    private UdpClient _udp;
    private DelayedEventScheduler _scheduler;
    internal DelayedEvent ScheduledResendEvent;

    private ushort messageId;

    
    private Action<PendingReliableMessage, int> resendActionAsync;
    private Action<Peer> maxRetriesExceededAction;
    internal ushort GetMessageId() => messageId;

    /// <summary>A pool of reusable <see cref="PendingMessage"/> instances.</summary>
    private static readonly ConcurrentQueue<PendingReliableMessage> pool = new ConcurrentQueue<PendingReliableMessage>();
       
    
    /// <summary>Retrieves a <see cref="PendingMessage"/> instance and initializes it.</summary>
    /// <param name="sequenceId">The sequence ID of the message.</param>
    /// <param name="message">The message that is being sent reliably.</param>
    /// <param name="connection">The <see cref="Connection"/> to use to send (and resend) the pending message.</param>
    /// <returns>An initialized <see cref="PendingMessage"/> instance.</returns>
    internal static PendingReliableMessage Create(ushort sequenceId, Message message, long sendTime,  Peer connection, DelayedEventScheduler scheduler)
    {
        PendingReliableMessage pendingMessage = RetrieveFromPool();
        pendingMessage.connection = connection;
        pendingMessage._scheduler = scheduler;

        pendingMessage.size = message.BytesInUse;
        Buffer.BlockCopy(message.GetBuffer(), 0, pendingMessage.data, 0, pendingMessage.size);
        pendingMessage.messageId = message.GetMessageID();
        pendingMessage.sendAttempts = 0;
        pendingMessage._acknowledged = false;
        pendingMessage.LastSentTicks = ReliableResendTimer.Now;
        return pendingMessage;
    }

    internal void RegisterEvents(Action<PendingReliableMessage, int> retryActionAsync, Action<Peer> attemptsExceededAction)
    {
        resendActionAsync = retryActionAsync;
        maxRetriesExceededAction = attemptsExceededAction;
    }


    
    /// <summary>Retrieves a <see cref="PendingMessage"/> instance from the pool. If none is available, a new instance is created.</summary>
    /// <returns>A <see cref="PendingMessage"/> instance.</returns>
    private static PendingReliableMessage RetrieveFromPool()
    {
        PendingReliableMessage message;
        if (pool.TryDequeue(out message))
        {
            message._isReleased = false;
            message._acknowledged = false;
        }
        else
            message = new PendingReliableMessage();

        return message;
    }


    public void RetrySend()
    {
        if (_acknowledged) return;

        if (sendAttempts > 13)
        {
            PnLog.LogWarning("No Ack received after 13 Retrys - Disconnecting client", true);
            maxRetriesExceededAction.Invoke(connection);
            connection.PendingReliableMessages.TryRemove(messageId, out _);
            Release();
            return;
        }
        
        resendActionAsync.Invoke(this, sendAttempts);
        sendAttempts++;
        LastSentTicks = Now;
        
    }
    
    public void AcknowledgeAndRelease()
    {
         _acknowledged = true;
         Release();
    }
    
    private void Release()
    {
        _scheduler.Remove(ScheduledResendEvent);
        _scheduler = null;
        connection = null;
        _scheduler = null;
        resendActionAsync = null;
        maxRetriesExceededAction = null;
        messageId = 0;
        size = 0;
        sendAttempts = 0;
       // _acknowledged = false;

        if (!_isReleased)
        {
            _isReleased = true;
            if(pool.Count < (BaseClientRegistry.totalStaticPeerCount + 1) * 5)
                pool.Enqueue(this); // Only add it if it's not already in the list, otherwise this method being called twice in a row for whatever reason could cause *serious* issues
        }
    }

    
    /// <summary>
    /// trim the pool, for instance on peer disconnection
    /// </summary>
    internal static void Trim()
    {
        if (pool.Count > (BaseClientRegistry.totalStaticPeerCount + 1)* 5)
        {
            int initialCount = pool.Count;
            while (pool.Count > (BaseClientRegistry.totalStaticPeerCount + 1) * 5)
            { 
                _ = pool.TryDequeue(out _);
            }
            PnLog.Log($"[PN PendingReliableMessage] trimmed pool from {initialCount} down to {pool.Count}");
        }
    }
    
  
    internal PendingReliableMessage()
    {
        data = new byte[Message.MaxSize];
    }
}
}