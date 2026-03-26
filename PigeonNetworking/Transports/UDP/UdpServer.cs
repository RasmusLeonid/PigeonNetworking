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
using PigeonNetworking.Server.ServerEvents;
using PigeonNetworking.Statistics;
using PigeonNetworking.Transports.UDP.Threading.Server;

namespace PigeonNetworking.Transports.UDP
{
    
    public class UdpServer : Server.Server
    {

        public UdpServer(byte laneId = 0) : base(laneId)
        {
         
        }

    private Thread _networkThread;
    internal ThreadManager _threadManager = new ThreadManager();
    internal AntiSpam _antiSpam;
    private AntiSpamOptions _antiSpamOptions = new AntiSpamOptions();
    
    const int SIO_UDP_CONNRESET = -1744830452;
   
    public static long Now => Stopwatch.GetTimestamp();
    public ClientRegistry _clientRegistry;

    private Task _serverTask;
    private CancellationTokenSource _cts;
    private long _lastResendTick = 0;

    private const int ackQueuSize = 300;
   

    private long lastAckFlush;
    private long lastMessageFlush;

    private int currentTickRate;
    private double currentTickDeltaTime;
    public override int GetIOThreadFrameRate() => currentTickRate;
    public override double GetIOThreadDeltaTime() => currentTickDeltaTime;
    private long lastTick;
    

    
    public override IEnumerable<Peer> Clients() => _clientRegistry.GetAllClients();

    public override bool TryGetPeer(ushort peerId, out Peer peer)
    {
        return _clientRegistry.TryGetById(peerId, out peer);
    }

    public override bool IsRunning()
    {
        return (_cts != null && !_cts.IsCancellationRequested);
    }

    public override int CurrentPeers() => _clientRegistry.Count;

    public override void SetAntiSpamConfig(AntiSpamOptions options)
    {
       _threadManager.DispatchToNetworkThread(() =>
       {
           _antiSpamOptions = options;
           _antiSpam.SetConfig(options);
       });
    }

    public override void Start(ushort port)
    {
        base.Start(port);
        _clientRegistry = new ClientRegistry(this);
        _cts = new CancellationTokenSource();
        _antiSpam = new AntiSpam(this, _antiSpamOptions);

        PnLog.Log($"Server started on port {port}.");

        _networkThread = new Thread(() =>
        {
            try
            {
                RunServer(_cts);
            }
            catch (SocketException sEx)
            {
                PnLog.LogException("[UDP Server] Socket exception: ", sEx);
                if (sEx.SocketErrorCode == SocketError.AddressAlreadyInUse)
                {
                    ThreadManager.DispatchToMainThread((() =>
                    {
                        OnServerFailed?.Invoke(this, new ServerFailedEvent(EServerFailedReason.PortAlreadyInUse));
                    }));
                }
            }
            catch (Exception e)
            {
                PnLog.LogException("[UDP Server] Exception: ", e);
            }
         
        });
        _networkThread.Start();
      
        OnServerReady?.Invoke(this, new ServerReadyEvent(new IPEndPoint(IPAddress.Any, Port).ToString()));
    }

    public override int GetPeerPing(ushort peerId)
    {
        if(_clientRegistry.TryGetById(peerId, out Peer peer))
        {
            return peer.HeartbeatRtt;
        }
        return -1;
    }

    private void RunServer(CancellationTokenSource cts)
    {
        _scheduler = new DelayedEventScheduler();
        _resendEventPool = new ResendEventPool(this);
        PnLog.Log($"[Server] Starting on Thread: {Thread.CurrentThread.ManagedThreadId}", true);
        Socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        
        // prevent ICMP response from throwing exceptions when remote peer is unreachable. Thank you Microsoft!
        Socket.IOControl((IOControlCode)SIO_UDP_CONNRESET, new byte[] { 0 }, null);
        Socket.Blocking = false;
        Socket.Bind(new IPEndPoint(IPAddress.Any, Port));
        
        IPEndPoint _receiveEndpoint;
        EndPoint remoteEP = new IPEndPoint(IPAddress.Any, 0);
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                int messagesProcessed = 0;
                while (Socket.Available > 0 && messagesProcessed < 200)
                {
                    messagesProcessed++;
                    var message = Message.Create();
                    int receivedBytes = Socket.ReceiveFrom(message.GetBuffer(), ref remoteEP);
                    EHeader headerId = (EHeader)message.GetBuffer()[0];
                    _receiveEndpoint = (IPEndPoint)remoteEP;
                    if (_antiSpam.IsBlocked(_receiveEndpoint))
                    {
                        message.Release();
                        continue;
                    }
                    EHeader header = headerId;
                    message.SetReceivedHeader(header);
                    HandleMessage(header, message, _receiveEndpoint); 
                    PnStatistics.IOReceived();
                }

                CheckTickRate();
                HandleSending();
                _scheduler.Tick();
                TickNagle();
                TickAckNagle();
                _antiSpam.Tick();
                if (ReliableResendTimer.ElapsedMs(lastHeartbeat) >= HeartbeatRate)
                {
                    _clientRegistry.TickHeartbeats();
                    lastHeartbeat = ReliableResendTimer.Now;
                }
                Thread.Sleep(TickDelay);
            }
            catch (SocketException sEx)
            {
                PnLog.LogException("[UDP Server] Socket exception: ", sEx);
                if (sEx.SocketErrorCode == SocketError.AddressAlreadyInUse)
                {
                    ThreadManager.DispatchToMainThread((() =>
                    {
                        OnServerFailed?.Invoke(this, new ServerFailedEvent(EServerFailedReason.PortAlreadyInUse));
                    }));
                }
            }
            catch (Exception ex)
            {
                PnLog.LogException("[UDP Server] Exception: ", ex);
               // throw;
            }
        }
        
        ThreadManager.DispatchToMainThread(() =>
        {
          _ = StopServer();
        });  
    }

    private void CheckTickRate()
    {
        long now = Stopwatch.GetTimestamp();
        double deltaTimeSeconds = (now - lastTick) / (double)Stopwatch.Frequency;
        lastTick = now;
        currentTickDeltaTime = deltaTimeSeconds;
        currentTickRate = (int)(1.0f / deltaTimeSeconds);
    }

    private  void TickAckNagle()
    {
        if (ReliableResendTimer.ElapsedMs(lastAckFlush) >= AckNagleTime)
        {
            FlushAcks();
            lastAckFlush = ReliableResendTimer.Now;
        }
    }

    private void TickNagle()
    {
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
                if (_clientRegistry.TryGetById((ushort)flushPeerId, out Peer peer))
                {
                    FlushNagleMessagesOnConnection(peer);
                }
                Interlocked.Exchange(ref flushPeerId, -1);
            }
        }
    }

    // called on main thread
    public override void Tick()
    {
        base.Tick();
        ThreadManager.UpdateMainThread();
    }

    
    // called ever tick on network thread
    private  void HandleSending()
    {
        try
        {
            _threadManager.UpdateNetworkThread();
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpServer.HandleSending(). Exception: ",e, true);
        }
    }

    private void HandleMessage(EHeader header, Message message, IPEndPoint endpoint)
    {
        if(_clientRegistry.TryGetByEndpoint(endpoint, out Peer peer) && peer.Accepted)
        {
            switch (header)
            {
                case EHeader.Unreliable:
                    ushort handlerId = message.ReadUShort();
                    PnStatistics.MessageReceived(EChannel.Unreliable);
                    var job = new ReceiveToMainThreadOnServerJob()
                    {
                        Message = message,
                        HandlerId = handlerId,
                        PeerId = peer.Id,
                        Caller = this
                    };
                    ServerThreadJobQueues.DispatchReceiveJobToMainThread(job);
                    break;
                case EHeader.Reliable:
                    HandleReliableMessage(header, message, peer.Id);
                    break;
                case EHeader.Ack:
                    HandleAck(message, peer);
                    break;
                case EHeader.Heartbeat:
                    HandleHeartbeat(message, peer);
                    break;
                case EHeader.ClientDisconnected:
                    HandleClientDisconnection(message, peer);
                    break;
                case EHeader.ReliableByteStream:
                    HandleReliableByteStreamChunk(message, peer);
                    break;
                case EHeader.NagledMessageBatch:
                    HandleNagleBatch(message, endpoint);
                    break;
            }
        }
        else
        {
            if (header == EHeader.ConnectRequest)
            {
                if (_clientRegistry.Count < MaxPeers)
                {
                    PnLog.DebugInfo("[UDP Server] Incoming connection request");
                    if (_clientRegistry.TryGetByEndpoint(endpoint, out Peer existing))
                    {
                        PnLog.LogWarning("Peer with IPEndpoint already exists - Ignoring ConnectRequest");
                        return;
                    }
                    Peer newClient = new Peer();
                    newClient.IpEndPoint = new IPEndPoint(endpoint.Address, endpoint.Port);
                    if (!_clientRegistry.TryAdd(ref newClient))
                    {
                        PnLog.LogException(new Exception("Failed to add endpoint to endpointToClient dictionary"));
                        RejectConnection(endpoint, ERejectionReason.ErrorAccepting);
                        return;
                    }
                    newClient.Accepted = true;
                    PnLog.LogDebug($"[PN UDP Server] Accepted client and assigned peerId: {newClient.Id}");
                    AcceptConnection(endpoint, newClient);
                }
                else
                {
                    RejectConnection(endpoint, ERejectionReason.ServerAtCapacity);
                }
            }
        }
    }

    private void RejectConnection(IPEndPoint endpoint, ERejectionReason rejectionReason)
    {
        Message message = Message.Create();
        message.SetHeader(EHeader.RejectConnection, false);
        message.WriteUShort((ushort)rejectionReason);
        try
        {
            for (int i = 0; i < 3; i++) // send 3 times to increase delivery chances
            {
                Socket.SendTo(message.GetBuffer(), message.BytesInUse, SocketFlags.None, endpoint);
                PnStatistics.IOSent();
            }
        }
        catch (Exception e)
        {
            PnLog.LogException($"Failed to send RejectConnection message to peer {endpoint.Address.ToString()}:{endpoint.Port}. Exception", e);
        }
   
        message.Release();
    }
    private void AcceptConnection(IPEndPoint endpoint, Peer peer)
    {
        Message message = Message.Create();
        message.SetHeader(EHeader.AcceptConnection, true);
        message.WriteUShort(peer.Id);
        SendToPeer(peer, message);
        PnLog.Log($"[Server] Accepted connection for endpoint: {endpoint.Address.ToString()}:{endpoint.Port} :message header: {(EHeader)message.GetBuffer()[0]}");
        NotifyPeersOfClientConnection(peer);
    }

    private void HandleHeartbeat(Message message, Peer client)
    {
        _clientRegistry.HandleHeartbeat(client, message);
       // PnLog.Log("[Server] Heartbeat received");
    }

    internal void KickPeer(Peer peer, EDisconnectionReason reason)
    {
        PnLog.LogWarning($"[Server] No heartbeats received from peer {peer.Id} after timeout time, disconnecting client");
        if(peer.IsInvalid) return;
        Message message = Message.Create();
        message.SetHeader(EHeader.Kick, false);
        message.WriteUShort((ushort)reason);
        try
        {
            for (int i = 0; i < 3; i++)
            {
                Socket.SendTo(message.GetBuffer(), message.BytesInUse, SocketFlags.None, peer.IpEndPoint);
                PnStatistics.IOSent();
            }
        }
        catch (Exception e)
        {
           PnLog.LogException($"Failed to send Kick message to peer {peer.Id}. Exception", e);
        }
        message.Release();
        NotifyPeersOfClientDisconnection(peer);
        _clientRegistry.TryRemove(peer);
    }
    
    
    /// <summary>
    /// Client wants to disconnect
    /// </summary>
    /// <param name="r_message"></param>
    /// <param name="client"></param>
    private void HandleClientDisconnection(Message r_message, Peer client)
    {
        EDisconnectionReason reason =  (EDisconnectionReason)r_message.ReadUShort();
        NotifyPeersOfClientDisconnection(client);
        _clientRegistry.TryRemove(client);
    }

    private void NotifyPeersOfClientConnection(Peer peer)
    {
        ThreadManager.DispatchToMainThread(() =>
        {
            OnClientConnected?.Invoke(this, new ClientConnectedEvent(peer.Id));
        });
        Message message = Message.Create();
        message.SetHeader(EHeader.ClientConnected, true);
        message.WriteUShort(peer.Id);
        SendToAll(message, peer.Id);
    }
    private void NotifyPeersOfClientDisconnection(Peer peer)
    {
        ThreadManager.DispatchToMainThread(() =>
        {
            OnClientDisconnected?.Invoke(this, new ClientDisconnectedEvent(peer.Id));
        });
        Message message = Message.Create();
        message.SetHeader(EHeader.ClientDisconnected, true);
        message.WriteUShort(peer.Id);
        SendToAll(message, peer.Id);
    }
    private void HandleAck(Message message, Peer peer)
    { 
        ushort batchedAckCount = message.ReadUShort();
        PnStatistics.AcksReceived(batchedAckCount);
        for (int i = 0; i < batchedAckCount; i++)
        {
            ushort messageId = message.ReadUShort();
            PnLog.LogDebug($"Ack received for message with Id: {messageId}", true);
            if(peer.PendingReliableMessages.TryGetValue(messageId, out PendingReliableMessage pendingMessage))
            {
                if (pendingMessage.GetMessageId() == messageId)
                {
                    pendingMessage.AcknowledgeAndRelease();
                    peer.PendingReliableMessages.TryRemove(messageId, out _);
                    PnLog.LogDebug($"Acknowledged and released Pending message with Id: {messageId}", true);
                }
            }
        }
        message.Release();
    }

    private void PushAck(ushort messageId, ushort clientId)
    {
        if (_clientRegistry.TryGetById(clientId, out Peer peer))
        {
            peer.QueuedAcks.Enqueue(messageId);
        }

        if (peer.QueuedAcks.Count >= 300)
        {
           FlushAcks();
        }
    }

    private void FlushAcks()
    {
        foreach (Peer peer in _clientRegistry.GetAllClients())
        {
            try
            {
                if (peer.QueuedAcks.Count > 0)
                {
                    PnStatistics.AcksSent(peer.QueuedAcks.Count);
                    Message message = Message.Create();
                    message.SetHeader(EHeader.Ack, false);
                    message.WriteUShort((ushort)peer.QueuedAcks.Count);
                    while (peer.QueuedAcks.TryDequeue(out ushort ack))
                    {
                        message.WriteUShort(ack);
                    }
                    Socket.SendTo(message.GetBuffer(), message.BytesInUse, SocketFlags.None, peer.IpEndPoint);
                    PnStatistics.IOSent();
                    message.Release();
                }
            }
            catch (Exception e)
            {
                PnLog.LogException("Exception in UdpServer.FlushAcks. Exception: ",e);
            }
        }
    }


    internal void HandleNagleBatch(Message batchMessage, IPEndPoint endpoint)
    { 
       ushort messageCount = batchMessage.ReadUShort(); 
       
       PnLog.DebugInfo($"[UdpServer]: Received batched message count: {messageCount}, total batch byte size: {batchMessage.BytesInUse} \n");
       for (int i = 0; i < messageCount; i++)
       {
          ushort expectedMessageSize = batchMessage.ReadUShort();
          
          Message message = Message.Create(); //todo: check if this is te best way to do this
          batchMessage.ReadBytes(message.GetBuffer(), expectedMessageSize);
          message.SetBytesInUse(expectedMessageSize);
          message.SetReceivedHeader((EHeader)message.GetBuffer()[0]);
          HandleMessage((EHeader)message.GetBuffer()[0], message, endpoint);
       }
       batchMessage.Release();
    }

    internal void FlushNagleMessages()
    {
        foreach (Peer peer in _clientRegistry.GetAllClients())
        {
            if (peer.NagledMessages > 0)
            {
                FlushNagleMessagesOnConnection(peer);
            }
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
       
        //PnLog.DebugInfo($"[UdpServer]: Adding message to nagle batch:  {(EHeader)message.GetBuffer()[0]}, size: {message.BytesInUse} \n");

        // message is nearly MTU size, we don't bother batching it
        if (message.BytesInUse >= Message.MaxSize - 20)
        {
            SendToPeer(peer, message, false);
            return;
        }
        // current messages + new message would exceed MTU size, we flush the existing queued messages first, also keep batch headers in mind.
        if (peer.NagleMessageWritePosition + message.BytesInUse + peer.NagledMessages * 2 + 20 >= Message.MaxSize)
            FlushNagleMessagesOnConnection(peer);


        peer.WriteMessageToNagleBuffer(message);
    }
    
    private void HandleReliableMessage(EHeader header, Message message, ushort peerId)
    {
        ushort messageId = message.ReadUShort();
        PushAck(messageId, peerId);
        PnStatistics.MessageReceived(EChannel.Reliable);
        if (_clientRegistry.TryGetById(peerId, out Peer peer))
        {
            if (!peer.RecentlyReceivedReliableMessageIDs.Contains(messageId))
            {
                while (peer.RecentlyReceivedReliableMessageIDs.Count >= 100)
                {
                    peer.RecentlyReceivedReliableMessageIDs.TryDequeue(out _);
                }
                peer.RecentlyReceivedReliableMessageIDs.Enqueue(messageId);
                
                ushort handlerId = message.ReadUShort();
                var job = new ReceiveToMainThreadOnServerJob()
                {
                    Message = message,
                    HandlerId = handlerId,
                    PeerId = peer.Id,
                    Caller = this
                };
                ServerThreadJobQueues.DispatchReceiveJobToMainThread(job);
            }
            else
            {
                PnStatistics.ReSentMessageReceived();
                message.Release();
            }
        }
        else
        {
            message.Release();
        }
    }

    public override void Stop()
    {
        StopServer();
    }

    private async Task StopServer()
    {
        try
        {
            _threadManager.DispatchToNetworkThread(() =>
            {
                foreach (var peer in _clientRegistry.GetAllClients())
                {
                    KickPeer(peer, EDisconnectionReason.ServerShutdown);
                }
            });
        }
        catch (Exception e)
        {
            PnLog.LogException("Exception in UdpServer.StopServer. Exception: ",e);
        }
        await Task.Delay(500);
        _cts.Cancel();
        Socket.Close();
        _resendEventPool = null;
        _clientRegistry = null;
        ThreadManager.DispatchToMainThread(() =>
        {
            _networkThread.Join();
            PnLog.Log("[UDP Server] Server stopped.");
        });
    }
    
    internal void RegisterResending(Message message, Peer peer)
    {
        PendingReliableMessage pendingMessage = PendingReliableMessage.Create(message.GetMessageID(), message, Now, peer, _scheduler);
        pendingMessage.RegisterEvents(ResendAction, MaxRetryAttemptsExceededForClient);
        peer.PendingReliableMessages[message.GetMessageID()] = pendingMessage;

        int resendTime = (int)(peer.HeartbeatRtt * PendingReliableMessage.RetryTimeMultiplier) + AckNagleTime;
        if(AllowMessageNagle) resendTime += MessageNagleTime;
        
        pendingMessage.ScheduledResendEvent =
            _scheduler.Schedule(_resendEventPool.Create(pendingMessage, resendTime));
       // PnLog.DebugInfo($"[Registering reliable message for resending] Header: {(EHeader)message.GetBuffer()[0]}  and peer: {peer.Id} Retry timer: {resendTime}", true);
    }

    private void ResendAction(PendingReliableMessage pMessage, int sendAttempts)
    {
        try
        {
            if(pMessage.GetPeer.IsInvalid) return;
            _ = Socket.SendTo(pMessage.GetBuffer(), pMessage.GetBufferSize(), SocketFlags.None, pMessage.GetPeer.IpEndPoint);
            PnStatistics.IOSent();
            PnStatistics.MessageResent();
            
            
            int resendTime = (int)(pMessage.GetPeer.HeartbeatRtt * PendingReliableMessage.RetryTimeMultiplier) + AckNagleTime + sendAttempts * 100;
            if(AllowMessageNagle) resendTime += MessageNagleTime;
            
            pMessage.ScheduledResendEvent =
                _scheduler.Schedule(_resendEventPool.Create(pMessage, resendTime));
            PnLog.LogWarning($"[ResendAction] Header: {(EHeader)pMessage.GetBuffer()[0]}, peer: {pMessage.GetPeer.Id} resend time {resendTime} messageId: {pMessage.GetMessageId()}", true);

        }
        catch (Exception e)
        {
            PnLog.LogException($"Failed to resend message with id: {pMessage.GetMessageId()} to peer {pMessage.GetPeer.Id}. Exception: ", e);
        }
      
    }
    private void MaxRetryAttemptsExceededForClient(Peer peer)
    {
        PnLog.LogWarning($"No Ack received from peer: {peer.Id} after max resend attempts. Disconnecting Client", true);
        KickPeer(peer, EDisconnectionReason.NoAcksReceived);
    }
    public override void Send(Message message, ushort peerId)
    {
        var job = new SendJob()
        {
            Message = message,
            PeerId = peerId,
            Caller = this
        };
        _threadManager.ServerThreadJobsDispatcher.DispatchSendToNetworkThread(job);
    }
    public override void SendToAll(Message message, ushort exceptClientId)
    {
        
        var job = new SendToAllExceptJob
        {
            Message = message,
            ExceptPeerId = exceptClientId,
            Caller = this
        };
        _threadManager.ServerThreadJobsDispatcher.DispatchToAllExceptJobToNetworkThread(job);
    }



    public override void SendToAll(Message message)
    {
        var job = new SendToAllJob
        {
            Message = message,
            Caller = this
        };
        _threadManager.ServerThreadJobsDispatcher.DispatchSendToAllJobToNetworkThread(job);
    }

    /// <summary>
    /// Send to a peer from the server thread on the server thread
    /// </summary>
    /// <param name="peer"></param>
    /// <param name="message"></param>
    internal void SendToPeer(Peer peer, Message message, bool release = true)
    {
        try
        {
            Socket.SendTo(message.GetBuffer(), message.BytesInUse, SocketFlags.None, peer.IpEndPoint);
            PnStatistics.IOSent();
        }
        catch (Exception e)
        {
            PnLog.LogException($"Failed to send message to peer {peer.Id}. Exception",e);
        }
        PnStatistics.IOSent();
        
        if(release)
            message.Release();
    }

    
    public override void SendReliableByteStream(byte[] data, ushort peerId)
    {
        SendReliableByteStreamTimed(data, peerId);
    }

    private async void SendReliableByteStreamTimed(byte[] data, ushort peerId)
    {
        
         if (_clientRegistry.TryGetById(peerId, out Peer peer))
         {
             byte streamId = peer.LastSentReliableByteStreamId;
             peer.LastSentReliableByteStreamId++;
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
                        PeerId = peerId,
                        Caller = this
                    };
                    _threadManager.ServerThreadJobsDispatcher.DispatchSendToNetworkThread(job);
                    await Task.Delay(10);
                    while (_threadManager.ServerThreadJobsDispatcher.sendJobsToServerThreadQueue.Count > 6)
                    {
                        await Task.Delay(10);
                    }
             
                    
                }
                PnLog.LogDebug($"[Pn Sending Reliable byte stream]: StreamId: {streamId}, chunkCount: {chunkCount}");
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
                    PeerId = peerId,
                    Caller = this
                };
                _threadManager.ServerThreadJobsDispatcher.DispatchSendToNetworkThread(job);
                PnLog.LogDebug($"[Pn Sending Reliable byte stream]: StreamId: {streamId}, chunkCount: (should be 1) {chunkCount}");
            }
     
            peer.LastSentReliableByteStreamId++;
        }
    }
    
    private void HandleReliableByteStreamChunk(Message message, Peer peer)
    {
        ushort messageId = message.ReadUShort();
        byte streamId = message.ReadByte();
        ushort chunkCount = message.ReadUShort();
        ushort chunkIndex = message.ReadUShort();
        byte[] chunk = message.ReadBytes();
        PnLog.LogDebug($"[Pn Handling Reliable byte stream chunk]: messageId {message}, streamId: {streamId}, chunkCount: {chunkCount}, chunkIndex: {chunkIndex}");
        PushAck(messageId, peer.Id);
       
        
        if (chunkCount <= 1)
        {
            ThreadManager.DispatchToMainThread(() =>
            {
                OnLargeByteStreamReceived?.Invoke(this, new LargeByteStreamReceivedOnServerEvent(chunk, peer.Id));
            });
            return;
        }
        if(!peer.receivedLargeByteStreamChunks.TryGetValue(streamId, out var chunksDict))
        {
            chunksDict = new SortedDictionary<ushort, byte[]>();
            peer.receivedLargeByteStreamChunks[streamId] = chunksDict;
        }
        chunksDict[chunkIndex] = chunk;
        if (chunksDict.Count == chunkCount)
        {
            if (peer.receivedLargeByteStreamChunks.TryGetValue(streamId, out var packetDict))
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
                    OnLargeByteStreamReceived?.Invoke(this, new LargeByteStreamReceivedOnServerEvent(stream, peer.Id));
                });
                
                peer.receivedLargeByteStreamChunks.Remove(streamId);
            }
        }
        
        message.Release();
    }

    public override void KickClient(ushort clientId, ushort customInfoId, string reasonInfo, long unixCustomTimestamp)
    {
        _threadManager.DispatchToNetworkThread(() =>
        {
            if (!_clientRegistry.TryGetById(clientId, out Peer peer))
            {
                PnLog.LogWarning($"");
                return;
            }
            if(peer.IsInvalid) return;
            PnLog.LogWarning($"No heartbeats received from peer {peer.Id} after timeout time, disconnecting client");
            Message message = Message.Create();
            message.SetHeader(EHeader.Kick, false);
            message.WriteUShort((ushort)EDisconnectionReason.CustomKick);
            message.WriteUShort(customInfoId);
            message.WriteSmartString(reasonInfo);
            message.WriteLong(unixCustomTimestamp);
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    Socket.SendTo(message.GetBuffer(), message.BytesInUse, SocketFlags.None, peer.IpEndPoint);
                    PnStatistics.IOSent();
                }
            }
            catch (Exception e)
            {
                PnLog.LogException($"Failed to send kick message to client {peer.Id}. Exception: ", e);
            }
     
            message.Release();
            NotifyPeersOfClientDisconnection(peer);
            _clientRegistry.TryRemove(peer);
        });
    }
}
}

