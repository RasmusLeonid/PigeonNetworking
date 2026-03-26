using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using PigeonNetworking.Core;
using PigeonNetworking.Core.Enums;
using PigeonNetworking.Helpers;

namespace PigeonNetworking.Transports.UDP
{
    public class ClientRegistry : BaseClientRegistry
{
    public ClientRegistry(UdpServer server)
    {
        _server = server;
    }
    private readonly ConcurrentDictionary<EndpointKey, Peer> endpointToClient = new();
    private readonly ConcurrentDictionary<ushort, Peer> clientIdToClient = new();

    private readonly UdpServer _server;
    
    public int Count => clientIdToClient.Count;
    
    internal bool TryAdd(ref Peer client)
    {
        var key = new EndpointKey(client.IpEndPoint);
        client.Id = GetNextClientId();
        client.EndpointKey = key; 
        bool success1 = endpointToClient.TryAdd(key, client);
        bool success2 = clientIdToClient.TryAdd(client.Id, client);
        if (!success1 || !success2)
        {
            if(!success2) endpointToClient.TryRemove(key, out _);
            if(!success1) clientIdToClient.TryRemove(client.Id, out _);
            
            PnLog.LogWarning($"[ClientRegistry] Failed to add client {client.Id} at {client.IpEndPoint}");
            return false;
        }

        Interlocked.Increment(ref totalStaticPeerCount);
        client.LastSentHeartbeat = ReliableResendTimer.Now;
      
        return true;
    }


    internal bool TryRemove(Peer peer)
    {
        if(peer.IsInvalid) return false; // already removed (kicked)
        bool removed1 = endpointToClient.TryRemove(peer.EndpointKey, out _);
        bool removed2 = clientIdToClient.TryRemove(peer.Id, out _);
        peer.IsInvalid = true;
        Interlocked.Decrement(ref totalStaticPeerCount);
        peer.Clear();
        TrimPools();
        return removed1 && removed2;
    }

    public bool TryGetByEndpoint(IPEndPoint endpoint, out Peer client)
    {
        var key = new EndpointKey(endpoint);
        return endpointToClient.TryGetValue(key, out client);
    }

    public bool TryGetById(ushort id, out Peer client)
        => clientIdToClient.TryGetValue(id, out client);

    public IEnumerable<Peer> GetAllClients()
        => clientIdToClient.Values;


    internal void TickHeartbeats()
    {
        foreach (var peer in clientIdToClient.Values)
        {
            if(!peer.IsInvalid)
                SendHeartbeats(peer);
        }
    }

    private void SendHeartbeats(Peer peer)
    {
        peer.LastHeartbeatId++;
        Message message = Message.Create();
        message.SetHeader(EHeader.Heartbeat, false);
        message.WriteUShort(peer.LastHeartbeatId);
        message.WriteUShort((ushort)peer.HeartbeatRtt);
        _server.SendToPeer(peer, message);
        peer.SentHeartbeats++;
        peer.LastSentHeartbeat = ReliableResendTimer.Now;
        //  PnLog.Log("[Server] Sending heartbeat");
        if (ReliableResendTimer.ElapsedMs(peer.LastReceivedHeartbeat) > _server.HeartbeatRate + _server.TimeoutTime && peer.SentHeartbeats > 10)
        {
            _server.KickPeer(peer, EDisconnectionReason.Timeout);
        }
    }
    internal void HandleHeartbeat(Peer peer, Message message)
    {
        
       ushort heartbeatId = message.ReadUShort();
       message.Release();
       if (peer.LastHeartbeatId == heartbeatId)
       {
           peer.SetTransportRTT((int)ReliableResendTimer.ElapsedMs(peer.LastSentHeartbeat));
           peer.SetHeartbeatRTT((int)ReliableResendTimer.ElapsedMs(peer.LastSentHeartbeat));
       }
       peer.LastReceivedHeartbeat = ReliableResendTimer.Now;
    }
}

}

