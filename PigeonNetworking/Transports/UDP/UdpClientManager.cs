using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using PigeonNetworking.Client.ClientEvents;
using PigeonNetworking.Core;
using PigeonNetworking.Core.Enums;
using PigeonNetworking.Helpers;
using PigeonNetworking.Statistics;
using PigeonNetworking.Transports.UDP.Threading.Client;

namespace PigeonNetworking.Transports.UDP
{
    
    public class UdpClientManager : Client.Client
{
    private IPEndPoint _serverEndpoint;
    private Task _serverTask;


    public UdpClientManager(byte laneId = 0) : base(laneId)
    {
    }
    
    public override bool IsConnected()
    {
        return _isRunning;
    }

    private int _valueIsRunningForInterlocked;
    private bool _isRunning
    {
        get => Interlocked.CompareExchange(ref _valueIsRunningForInterlocked , 0, 0) != 0;
        set => Interlocked.Exchange(ref _valueIsRunningForInterlocked , value ? 1 : 0);
    }
    
    private long _lastResendTick = 0;

    protected long lastHeartbeat;

    private bool _connectionRejected = false;


    protected ThreadManager _threadManager = new ThreadManager();
    protected Thread _networkThread;
    
    private const int ackQueuSize = 300;
    
    private long lastAckFlush;
    private long lastMessageFlush;
    
    protected Queue<ushort> ackQueue = new Queue<ushort>();
    
    private long lastTick;
    private int currentTickRate;
    private double currentTickDeltaTime;
    public override int GetIOThreadFrameRate() => currentTickRate;
    public override double GetIOThreadDeltaTime() => currentTickDeltaTime;
    
    public override void Start()
    {
        base.Start();
        PnLog.Log($"Client started");
    }

    public override void Connect(string connectInfo)
    {
        PnLog.Log("[Udp ClientManager] Attempting to connect");
        string ipAddress;
        ushort port = 7777;
        string[] split = connectInfo.Split(':');
        if (split.Length > 0)
        {
            ipAddress = split[0];
            if (split.Length > 1)
            {
                if (!ushort.TryParse(split[1], out port))
                {
                    PnLog.LogException(new Exception("ConnectInfo is not a valid IP:Port string."));
                    return;
                }
            }
        }
        else
        {
            PnLog.LogException(new Exception("ConnectInfo is not a valid IP:Port string."));
            return;
        }
        
        try
        {
            _serverEndpoint = new IPEndPoint(IPAddress.Parse(ipAddress), port);

            serverPeer = new Peer
            {
                IpEndPoint = _serverEndpoint,
                LastReceivedHeartbeat = ReliableResendTimer.Now
            };
        }
        catch (SocketException ex)
        {
            PnLog.LogException("Socket error", ex);
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpClientManager.Connect(SocketInfoBase baseSocket). Exception:",e);
            throw;
        }

        _isRunning = true;
        _networkThread = new Thread(() =>
        {
            RunClient(_serverEndpoint);
        });
        _networkThread.Start();
       
    }

    private async Task EstablishConnection()
    {
        PnLog.Log($"[Udp ClientManager] Establishing connection to IP: {serverPeer.IpEndPoint.Address.ToString()}:{serverPeer.IpEndPoint.Port}", true);
        await Task.Delay(2000);
        int i = 0;
        _connectionRejected = false;
        _isConnected = false;
        while (!_isConnected && i++ < 5 && !_connectionRejected)
        {
            SendConnectRequest();
            await Task.Delay(2000);
        }
        if (!_isConnected)
        {
            ThreadManager.DispatchToMainThread(() =>
            {
                Stop();
                OnFailedToConnect?.Invoke(this, new ConnectionRejectedFromServerEvent(ERejectionReason.FailedToConnect));
            });
        }
    }

    public override void Disconnect()
    {
        DisconnectFromServer(EDisconnectionReason.Intentional);
        Message.Trim();
        PendingReliableMessage.Trim();
    }

    private  void DisconnectFromServer(EDisconnectionReason reason)
    {
        Message message = Message.Create();
        message.SetHeader(EHeader.ClientDisconnected, false);
        message.WriteUShort((ushort)reason);
        _isConnected = false;
        for (int i = 0; i < 3; i++)
        {
           Socket?.Send(message.GetBuffer(), 0, message.BytesInUse, SocketFlags.None);
           PnStatistics.IOSent();
        }
        message.Release();
        
        ThreadManager.DispatchToMainThread(() =>
        {
            OnDisconnectedFromServer?.Invoke(this, new DisconnectedFromServerEvent(reason));
            Stop();
        });
      
    }

    public override void Stop()
    {
        if (_networkThread == Thread.CurrentThread)
        {
            PnLog.LogError("Tried to Call Main thread function 'Stop()' directly from network thread. This is not allowed and should never happen!");
            return;
        }
        PnLog.LogWarning("Stopping client");
        _isRunning = false;
        Socket?.Close();
        _scheduler = null;
        _isConnected = false;
        _resendEventPool = null;
        _networkThread?.Join();
        base.Stop();
    }

    private void SendConnectRequest()
    {
        var message = Message.Create();
        message.SetHeader(EHeader.ConnectRequest, false);
        Socket.Send(message.GetBuffer(),  0,message.BytesInUse, SocketFlags.None);
        PnStatistics.IOSent();
        message.Release();
        PnLog.Log("[Client] Sending connection Request", true);
    }
    
    private void RunClient(IPEndPoint serverEndpoint)
    {
        _scheduler = new DelayedEventScheduler();
        _resendEventPool = new ResendEventPool(null);
        PnLog.Log($"[Client] Starting on Thread: {Thread.CurrentThread.ManagedThreadId}", true);
        Socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        Socket.Blocking = false;
        Socket.Bind(new IPEndPoint(IPAddress.Any, 0));
        Socket.Connect(serverEndpoint);
        _ = EstablishConnection();
        try
        {
            while (_isRunning)
            {
                int messagesProcessed = 0;
                while (Socket.Available > 0 && messagesProcessed < 150)
                {
                    try
                    {
                        messagesProcessed++;
                        var message = Message.Create();
                        int receivedBytes = Socket.Receive(message.GetBuffer());
                        message.SetBytesInUse(receivedBytes);
                        EHeader header = (EHeader)message.GetBuffer()[0];
                        message.SetReceivedHeader(header);
                        HandleMessage(header, message);
                        PnStatistics.IOReceived();
                    }
                    catch (Exception e)
                    {
                       PnLog.LogException("Exception in Socket polling loop: ",e);
                        throw;
                    }
                  
                }

                CheckTickRate();
                HandleSending();
                TickAckNagle();
                TickNagle();
                _scheduler.Tick();
                if (ReliableResendTimer.ElapsedMs(lastHeartbeat) >= HeartbeatRate)
                {
                    lastHeartbeat = ReliableResendTimer.Now;
                    if (_isConnected && ReliableResendTimer.ElapsedMs(serverPeer.LastReceivedHeartbeat) >
                        HeartbeatRate + TimeoutTime)
                    {
                        PnLog.LogWarning($"[Client] No heartbeats received in time. Timing out", true);
                        DisconnectFromServer(EDisconnectionReason.LocalTimeout);
                    }
                }
                Thread.Sleep(TickDelay);
            }

            PnLog.LogWarning("[Client] Disconnected", true);
        }
        catch (SocketException se)
        {
            EDisconnectionReason reason = EDisconnectionReason.TransportError;
            if (se.SocketErrorCode == SocketError.ConnectionReset)
                reason = EDisconnectionReason.RemotePeerReset;
            
            ThreadManager.DispatchToMainThread(() =>
            {
                OnDisconnectedFromServer?.Invoke(this, new DisconnectedFromServerEvent(reason));
                Stop();
            });
            PnLog.LogException("SocketException in UdpClientManager.RunClient(). Exception: ",se, true);
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpClientManager.RunClient(). Exception: ",e, true);
        }
    }
    private void CheckTickRate()
    {
        long now = Stopwatch.GetTimestamp();
        double deltaTimeSeconds = (now - lastTick) / (double)Stopwatch.Frequency;
        lastTick = now;
        currentTickDeltaTime = deltaTimeSeconds;
        currentTickRate = (int)(1.0f / deltaTimeSeconds);
    }
    private void TickAckNagle()
    {
        if (ReliableResendTimer.ElapsedMs(lastAckFlush) >= AckNagleTime)
        {
            FlushAcks();
            lastAckFlush = ReliableResendTimer.Now;
        }
    }
    
    private void TickNagle()
    {
        if(!AllowMessageNagle) return;
        if (flushAll > 0)
        {
            FlushNagleMessages();
            lastMessageFlush = ReliableResendTimer.Now;
            Interlocked.Exchange(ref flushAll, -1);
            return;
        }

        if (ReliableResendTimer.ElapsedMs(lastMessageFlush) >= MessageNagleTime)
        {
            FlushNagleMessages();
            lastMessageFlush = ReliableResendTimer.Now;
        }
        else
        {
            if (flushPeerId > 0)
            {
                FlushNagleMessagesOnConnection(serverPeer);
                Interlocked.Exchange(ref flushPeerId, -1);
            }
        }
    }
    private  void HandleSending()
    {
        try
        {
            _threadManager.UpdateNetworkThread();
       
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpClientManager.HandleSending(). Exception: ", e, true);
        }
    }
    private void HandleMessage(EHeader header, Message message)
    {
        switch (header)
        {
            case EHeader.Unreliable:
                ushort handlerId = message.ReadUShort();
                PnStatistics.MessageReceived(EChannel.Unreliable);
                var job = new DispatchToMainOnClientReceiveJob()
                {
                    Message = message,
                    HandlerId = handlerId,
                    Caller = this
                };
                ClientThreadJobQueues.DispatchReceiveToMainThread(job);
                break;
            case EHeader.Reliable:
                HandleReliableMessage(header, message);
                break;
            case EHeader.Ack:
                HandleAck(message);
                break;
            case EHeader.Heartbeat:
                HandleHeartbeat(message);
                break;
            case EHeader.ClientDisconnected:
                HandleClientDisconnection(message);
                break;
            case EHeader.ClientConnected:
                HandleClientConnected(message);
                break;
            case EHeader.AcceptConnection:
                _isConnected = true;
                ThreadManager.DispatchToMainThread(() =>
                {
                    OnConnectedToServer?.Invoke(this, new ConnectedToServerEvent());
                });
                ushort messageId = message.ReadUShort();
                PushAck(messageId);
                ushort ourPeerId = message.ReadUShort();
                _id = ourPeerId;
                serverPeer.Id = ourPeerId;
                message.Release();
                PnLog.Log($"[Client] Connection Accepted from Server and received our PeerId: {_id}", true);
                break;
            case EHeader.RejectConnection:
                _isConnected = false;
                ERejectionReason rejectionReason = (ERejectionReason)message.ReadUShort();
                OnFailedToConnect?.Invoke(this, new ConnectionRejectedFromServerEvent(rejectionReason));
                break;
            case EHeader.Kick:
                HandleKickedFromServer(message);
                break;
            case EHeader.ReliableByteStream:
                HandleReliableByteStreamChunk(message);
                break;
            case EHeader.NagledMessageBatch:
                HandleNagleBatch(message);
                break;
        }
    }

    public override void Tick()
    {
        base.Tick();
        ThreadManager.UpdateMainThread();
    }

    private void FlushAcks()
    {
        try
        {
            PnStatistics.AcksSent(ackQueue.Count);
            if (ackQueue.Count > 0)
            {
                Message message = Message.Create();
                message.SetHeader(EHeader.Ack, false);
                message.WriteUShort((ushort)ackQueue.Count);
                while (ackQueue.TryDequeue(out ushort ack))
                {
                    message.WriteUShort(ack);
                    PnLog.LogDebug($"Sending Ack for reliable message with id: {ack}", true);
                }
                Socket.Send(message.GetBuffer(), 0, message.BytesInUse, SocketFlags.None);
                PnStatistics.IOSent();
                message.Release();
            }
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpClientManager.FlushAcks(). Exception: ",e, true);
        }
    }


    private  void HandleHeartbeat(Message received)
    {
        ushort heartbeatId = received.ReadUShort();
        ushort RTT = received.ReadUShort();
        serverPeer.SetHeartbeatRTT(RTT);
        serverPeer.SetTransportRTT(RTT);
        serverPeer.LastReceivedHeartbeat = ReliableResendTimer.Now;
        received.Release();
        
        Message message = Message.Create();
        message.SetHeader(EHeader.Heartbeat, false);
        message.WriteUShort(heartbeatId);
        Socket.Send(message.GetBuffer(), 0, message.BytesInUse, SocketFlags.None);
        PnStatistics.IOSent();
        message.Release();
    }

    private void HandleClientConnected(Message message)
    {
        PnLog.Log("[Client] Other Client connected", true);
        PushAck(message.ReadUShort());
        ushort clientId = message.ReadUShort();
        message.Release();
        ThreadManager.DispatchToMainThread(() =>
        {
            OnOtherClientConnected?.Invoke(this, new OtherClientConnectedEvent(clientId));
        });
    }
    private void HandleClientDisconnection(Message message)
    {
        PushAck(message.ReadUShort());
        ushort clientId = message.ReadUShort();
        message.Release();
        ThreadManager.DispatchToMainThread(() =>
        {
            OnOtherClientDisconnected?.Invoke(this, new OtherClientDisconnectedEvent(clientId));
        });
    }

    private void HandleKickedFromServer(Message message)
    {
        EDisconnectionReason reason = (EDisconnectionReason)message.ReadUShort();
        ThreadManager.DispatchToMainThread(() =>
        {
            OnDisconnectedFromServer?.Invoke(this, new DisconnectedFromServerEvent(reason, message));
            Stop();
        });
      
    }
    private void HandleReliableMessage(EHeader header, Message message)
    {
        //PnLog.LogDebug("Received reliable message", true);
        ushort messageId = message.ReadUShort();
        PushAck(messageId);
        PnStatistics.MessageReceived(EChannel.Reliable);
        if (!serverPeer.RecentlyReceivedReliableMessageIDs.Contains(messageId))
        {
            while (serverPeer.RecentlyReceivedReliableMessageIDs.Count >= 100)
            {
                serverPeer.RecentlyReceivedReliableMessageIDs.TryDequeue(out _);
            }
            serverPeer.RecentlyReceivedReliableMessageIDs.Enqueue(messageId);
            
            ushort handlerId = message.ReadUShort();
            var job = new DispatchToMainOnClientReceiveJob()
            {
                Message = message,
                HandlerId = handlerId,
                Caller = this
            };
            ClientThreadJobQueues.DispatchReceiveToMainThread(job);
        }
        else
        {
            PnStatistics.ReSentMessageReceived();
            message.Release();
        }
    }
    private void HandleAck(Message message)
    { 
        ushort ackCount = message.ReadUShort();
        PnStatistics.AcksReceived(ackCount);
        for (int i = 0; i < ackCount; i++)
        {
            ushort messageId = message.ReadUShort();
            PnLog.LogDebug($"[Udp Client]: Handle Ack for messageId: {messageId}", true);
            if(serverPeer.PendingReliableMessages.TryGetValue(messageId, out PendingReliableMessage pendingMessage))
            {
                if (pendingMessage.GetMessageId() == messageId)
                {
                    _ =  serverPeer.PendingReliableMessages.TryRemove(messageId, out _);
                    pendingMessage.AcknowledgeAndRelease();
                }
            }
        }
        message.Release();
    }
    internal void HandleNagleBatch(Message batchMessage)
    { 
       ushort messageCount = batchMessage.ReadUShort(); 
       PnLog.DebugInfo($"[UdpClient]: Received batched message count: {messageCount}, total batch byte size: {batchMessage.BytesInUse} \n");
       for (int i = 0; i < messageCount; i++)
       {
          ushort expectedMessageSize = batchMessage.ReadUShort();
          
          Message message = Message.Create(); 
          batchMessage.ReadBytes(message.GetBuffer(), expectedMessageSize);
          message.SetBytesInUse(expectedMessageSize);
          message.SetReceivedHeader((EHeader)message.GetBuffer()[0]); 
          PnLog.DebugInfo($"Received batched message, expected size: {expectedMessageSize} - Header {(EHeader)message.GetBuffer()[0]}");
          HandleMessage((EHeader)message.GetBuffer()[0], message);
       }
       batchMessage.Release();
    }

    internal void FlushNagleMessages()
    {
        if (serverPeer.NagledMessages > 0)
        {
            FlushNagleMessagesOnConnection(serverPeer);
        }
    }

    internal void FlushNagleMessagesOnConnection(Peer peer)
    {
        if(peer.NagledMessages == 0) return;
        BinaryPrimitives.WriteUInt16LittleEndian(peer.NagleMessageBuffer.AsSpan(1), (ushort)peer.NagledMessages);
        try
        {
            if (peer.NagleMessageWritePosition >= Message.MaxSize)
            {
                PnLog.LogError("Maximum message size exceeded, while trying to send batched nagle message. This will lead to errors on the receiving end.");
            }
            Socket.SendTo(peer.NagleMessageBuffer, peer.NagleMessageWritePosition, SocketFlags.None, peer.IpEndPoint);
            
            PnStatistics.IOSent();
        }
        catch (SocketException se)
        {
            PnLog.LogException("[UdpServer] SocketException in FlushMessagesOnConnection", se);
        }
        catch (Exception e)
        {
            PnLog.LogException("[UdpServer] Exception in FlushMessagesOnConnection", e);
        }
        finally
        {
            peer.ResetNagleMessageBuffer();
        }
     
    }

    internal void AddMessageToNagleSendQueue(Message message, Peer peer)
    {
        // message is nearly MTU size, we don't bother batching it
        if (message.BytesInUse >= Message.MaxSize - 20)
        {
            SendToServerFromIOThread(message, false);
            return;
        }
        // current messages + new message would exceed MTU size, we flush the existing queued messages first, also keep batch headers in mind.
        if (peer.NagleMessageWritePosition + message.BytesInUse + peer.NagledMessages * 2 + 20 >= Message.MaxSize)
            FlushNagleMessagesOnConnection(peer);


        peer.WriteMessageToNagleBuffer(message);
    }

    /// <summary>
    /// Send to a peer from the server thread on the server thread
    /// </summary>
    /// <param name="peer"></param>
    /// <param name="message"></param>
    internal void SendToServerFromIOThread(Message message, bool release = true)
    {
        try
        {
            Socket.Send(message.GetBuffer(), 0, message.BytesInUse, SocketFlags.None);
            PnStatistics.IOSent();
        }
        catch (Exception e)
        {
            PnLog.LogException($"Failed to send message to peer {serverPeer.Id}. Exception",e);
        }
        PnStatistics.IOSent();
        if(release)
            message.Release();
    }
    public override void SendReliableByteStream(byte[] data)
    {
        SendReliableByteStreamTimed(data);
    }

    private async void SendReliableByteStreamTimed(byte[] data)
    {
        byte streamId = serverPeer.LastSentReliableByteStreamId;
        serverPeer.LastSentReliableByteStreamId++;    

        int chunkSize = Message.MaxSize - 20;
        ushort chunkCount = (ushort)Math.Ceiling((float)data.Length / (float)chunkSize);
        if (chunkCount >= ushort.MaxValue)
        {
            throw new Exception($"Cannot send a reliable byte stream larger than {ushort.MaxValue -1} chunks");
        }
        if (chunkCount > 1)
        {
            List<byte[]> chunks = new List<byte[]>();
            int offset = 0;
            while (offset < data.Length)
            {
                int size = Math.Min(chunkSize, data.Length - offset);
                byte[] chunk = new byte[size];
                Array.Copy(data, offset, chunk, 0, size);
                chunks.Add(chunk);
                offset += size;
            }

            for (int i = 0; i < chunks.Count; i++)
            {
                Message message = Message.Create();
                message.SetHeader(EHeader.ReliableByteStream, true);
                message.SetChannel(EChannel.Reliable);
                message.WriteByte(streamId); // write the stream id
                message.WriteUShort((ushort)chunks.Count); // write the number of chunks
                message.WriteUShort((ushort)i); // write the index of the chunk
                message.WriteBytes(chunks[i]); // write the chunk data
                    
                var job = new SendJob()
                {
                    Message = message,
                    Caller = this
                };
                _threadManager.ClientThreadJobDispatcher.DispatchSendJobToNetworkThread(job);
             
                await Task.Delay(10);
                PnLog.LogDebug($"[Pn Sending Reliable byte stream chunk]: streamId: {streamId} chunkIndex: {i}, chunkLength: {chunks[i].Length}, message buffer: {message.GetBuffer().Length}");
                while (_threadManager.ClientThreadJobDispatcher.sendToNetworkThreadQueue.Count > 6)
                {
                    await Task.Delay(10); // Wait for backlog to clear
                }
            }
            PnLog.LogDebug($"[Pn Sending Reliable byte stream]: StreamId: {streamId}, chunkCount: {chunkCount} data: {data.Length}");
     
        }
        else
        {
            Message message = Message.Create();
            message.SetHeader(EHeader.ReliableByteStream, true);
            message.SetChannel(EChannel.Reliable);
            message.WriteByte(streamId);
            message.WriteUShort(1);
            message.WriteUShort(0);
            message.WriteBytes(data);
                
            var job = new SendJob()
            {
                Message = message,
                Caller = this
            };
            _threadManager.ClientThreadJobDispatcher.DispatchSendJobToNetworkThread(job);
            PnLog.LogDebug($"[Pn Sending Reliable byte stream]: StreamId: {streamId}, chunkCount: {chunkCount}");
        }
        
    }

    private void HandleReliableByteStreamChunk(Message message)
    {
        PnLog.LogDebug("[Pn Handling Reliable byte stream chunk]");
        ushort messageId = message.ReadUShort();
        byte streamId = message.ReadByte();
        ushort chunkCount = message.ReadUShort();
        ushort chunkIndex = message.ReadUShort();
        byte[] chunk = message.ReadBytes();
        
        PnLog.LogDebug($"[Pn Handling Reliable byte stream chunk]: messageId {message}, streamId: {streamId}, chunkCount: {chunkCount}, chunkIndex: {chunkIndex}");

        PushAck(messageId);
     
        
        if (chunkCount <= 1)
        {
            ThreadManager.DispatchToMainThread(() =>
            {
                OnLargeByteStreamReceived?.Invoke(this, new LargeByteStreamReceivedEvent(chunk));
            });
            return;
        }
        if(!serverPeer.receivedLargeByteStreamChunks.TryGetValue(streamId, out var chunksDict))
        {
            chunksDict = new SortedDictionary<ushort, byte[]>();
            serverPeer.receivedLargeByteStreamChunks[streamId] = chunksDict;
        }
        chunksDict[chunkIndex] = chunk;
        if (chunksDict.Count == chunkCount)
        {
            if (serverPeer.receivedLargeByteStreamChunks.TryGetValue(streamId, out var packetDict))
            {
                byte[] stream = new byte[packetDict.Values.Sum(x => x.Length)];
                int offset = 0;
                foreach (var c in packetDict.Values)
                {
                    Array.Copy(c, 0,  stream , offset, c.Length);
                    offset += c.Length;
                }
                    
                ThreadManager.DispatchToMainThread(() =>
                {
                    OnLargeByteStreamReceived?.Invoke(this, new LargeByteStreamReceivedEvent(stream));
                });
                
                serverPeer.receivedLargeByteStreamChunks.Remove(streamId);
            }
        }
        message.Release();
    }

    private void PushAck(ushort messageId)
    {
        ackQueue.Enqueue(messageId);
        if (ackQueue.Count > ackQueuSize)
        {
            FlushAcks();
        }
    }
    
    public override void Send(Message message)
    {
        base.Send(message);
        
        var job = new SendJob()
        {
            Message = message,
            Caller = this
        };
        _threadManager.ClientThreadJobDispatcher.DispatchSendJobToNetworkThread(job);
    }
    
   
    internal void RegisterResending(Message message, Peer client)
    {
        PendingReliableMessage pendingMessage = PendingReliableMessage.Create(message.GetMessageID(), message, ReliableResendTimer.Now, client, _scheduler);
        pendingMessage.RegisterEvents(ResendAction, MaxRetryAttemptsExceededForClient);
        client.PendingReliableMessages[message.GetMessageID()] = pendingMessage;

        int resendTime = (int)(client.HeartbeatRtt * PendingReliableMessage.RetryTimeMultiplier + AckNagleTime);
        if (AllowMessageNagle) resendTime += MessageNagleTime;
        
         pendingMessage.ScheduledResendEvent = _scheduler.Schedule(_resendEventPool.Create(pendingMessage, resendTime));
        PnLog.LogDebug($"Register resending timer: {(int)resendTime}", true);
    }

    //todo: prevent resend messages from bypassing nagle?
    private void ResendAction(PendingReliableMessage pMessage, int sendAttempts)
    {
        try
        {
            Socket.Send(pMessage.GetBuffer(), 0, pMessage.GetBufferSize(), SocketFlags.None);
            PnStatistics.IOSent();
            PnStatistics.MessageResent();
        }
        catch (Exception e)
        {
            PnLog.LogException($"Exception in UdpClientManager.ResendAction(). Socket null? {Socket == null}, pMessage null? {pMessage == null}  Exception: ", e, true);
        }


        int resendTime = (int)(pMessage.GetPeer.HeartbeatRtt * PendingReliableMessage.RetryTimeMultiplier) + sendAttempts * 100;
        resendTime += AckNagleTime;
        if(AllowMessageNagle) resendTime += MessageNagleTime;
                    
        pMessage.ScheduledResendEvent = _scheduler.Schedule(_resendEventPool.Create(pMessage, resendTime));
        PnLog.LogWarning($"[Udp Client] Resending reliable message {pMessage.GetMessageId()} resend time {resendTime}", true);
    }
    private void MaxRetryAttemptsExceededForClient(Peer client)
    {
        PnLog.LogWarning("[Udp Client] MaxResend attempts exceeded", true);
        DisconnectFromServer(EDisconnectionReason.LocalNoAcksReceived);
    }
}
}

