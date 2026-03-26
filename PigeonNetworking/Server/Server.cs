using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using PigeonNetworking.Core;
using PigeonNetworking.Exceptions;
using PigeonNetworking.Server.ServerEvents;

namespace PigeonNetworking.Server
{
    public abstract class Server : BaseNetEndpoint
    {
        
        /// <summary>
        /// Get all connected peers
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<Peer> Clients();

        
        /// <summary>
        /// Get the amount of connected peers
        /// </summary>
        /// <returns>int32 peer count</returns>
        public abstract int CurrentPeers();



        /// <summary>
        /// Get the tick rate (Frames per second) of the Network thread loop. This will only be set on multithreaded transports.
        /// Tick rate is set to 1000 Hz by default.
        /// Intended for debugging.
        /// </summary>
        /// <returns>Returns -1 if not set</returns>
        public virtual int GetIOThreadFrameRate() { return -1; }

        
        /// <summary>
        /// Get the DeltaTime between each tick of the network thread loop in seconds. This will only be set on multithreaded transports.
        /// Tick rate is locked to 100 Hz by default.
        /// Intended for debugging.
        /// </summary>
        /// <returns>Returns -1 if not set</returns>
        public virtual double GetIOThreadDeltaTime() { return -1; }
        
        /// <summary>
        /// The port the server runs on (virtual port if SDR)
        /// </summary>
        public ushort Port { get; private set; }

        
        /// <summary>
        /// Maximum amount of peers that can be connected at a given time
        /// </summary>
        public int MaxPeers { get; private set; }


        /// <summary>
        /// Set max peers/clients value
        /// Default is 20
        /// </summary>
        /// <param name="value"></param>
        public void SetMaxPeers(int value) => MaxPeers = value;


        /// <summary>
        /// Invoked when a client connects to the server and the peer is ready
        /// </summary>
        public EventHandler<ClientConnectedEvent> OnClientConnected;
    
        /// <summary>
        /// Invoked when a client disconnected from the server and transport for the peer stopped
        /// </summary>
        public EventHandler<ClientDisconnectedEvent> OnClientDisconnected;


        /// <summary>
        /// Invoked when the server is ready for clients to connect, good for knowing when the Steam Transport is ready to get the steamId to connect to
        /// by doing SteamGameServerNetworkingSockets.GetIdentity(out SteamNetworkingIdentity id) and get the CSteamID from the identity object
        /// </summary>
        public EventHandler<ServerReadyEvent> OnServerReady;

        /// <summary>
        /// Invoked when the server runs into an error, for instance, when the UDP Port we are trying to start on is already occupied.
        /// </summary>
        public EventHandler<ServerFailedEvent> OnServerFailed;
        
        
        /// <summary>
        /// Invoked when a large reliable byte array is received, Sent by calling SendReliableByteStream on remote Peer
        /// </summary>
        public EventHandler<LargeByteStreamReceivedOnServerEvent> OnLargeByteStreamReceived;
    
        internal ConcurrentDictionary<ushort, Action<ushort, Message>> MessageHandlers = new ConcurrentDictionary<ushort, Action<ushort, Message>>();
 
        /// <summary>
        /// Initialize the Server object
        /// </summary>
        /// <param name="laneId">Setting a lane will allow you to run multiple independent server instances, receiving messages on the specified lane only.
        /// Default lane is 0 and does not have to be specified</param>
        public Server(byte laneId)
        {
            MaxPeers = 20;
            Lane = laneId;
            RegisterHandlers();
        }
    
        /// <summary>
        /// used by the transport to determine heartbeat and ping check rates
        /// </summary>
        protected long lastHeartbeat;



        
        /// <summary>
        /// Is server running
        /// </summary>
        /// <returns></returns>
        public abstract bool IsRunning();
    
        /// <summary>
        /// Get the RTT of a peer in milliseconds
        /// </summary>
        /// <param name="peerId"></param>
        /// <returns>returns ping in milliseconds, returns -1 if the peer could not be found</returns>
        public abstract int GetPeerPing(ushort peerId);

        
        /// <summary>
        /// returns true if the peer with the specifiedId exists and retunrs the peer object as out param peer.
        /// returns ture, if no peer with the given Id exists.
        /// Please do not modify anything on the object, if you are not sure what you are doing.
        /// </summary>
        /// <param name="peerId"></param>
        /// <returns></returns>
        public abstract bool TryGetPeer(ushort peerId, out Peer peer);
    
    
        /// <summary>
        /// Start the server
        /// </summary>
        /// <param name="port"></param>
        public virtual void Start(ushort port)
        {
            Port = port;
        }


        /// <summary>
        /// Disconnect all clients and Stop the server
        /// </summary>
        public virtual void Stop()
        {
            MessageHandlers.Clear();
        }
  

    
        /// <summary>
        /// Call this each frame to Run callbacks and update thread dispatcher
        /// </summary>
        public virtual void Tick(){}



        /// <summary>
        ///  Send to a single peer
        /// </summary>
        /// <param name="message"></param>
        /// <param name="peerId">clientId of the client we want to send to</param>
        public abstract void Send(Message message, ushort peerId);
 

        /// <summary>
        ///  Send a message to all, except for one peer
        /// </summary>
        /// <param name="message"></param>
        /// <param name="exceptClientId">clientId of the client we don't want to send to</param>
        public abstract void SendToAll(Message message, ushort exceptClientId);

        /// <summary>
        ///  Send a message to all peers
        /// </summary>
        /// <param name="message"></param>
        public abstract void SendToAll(Message message);

    
        
        
        /// <summary>
        /// only use this, if you need to send a byte array that is bigger then the max message size.
        /// This function should not be used too frequently and is intended to handle edge cases, where
        /// the data we need to send does not fit into the Message class' buffer/MTU.
        /// This sends a large byte array, by splitting it into as many segments as needed for transport and
        /// ensures all chunks are received and ordered, before the buffer is dispatched with an OnLargeByteStreamReceived event.
        /// When using this function, you will need to manage handling the data yourself, by e.g. writing your own header
        /// into the byte array, before sending it.
        /// </summary>
        /// <param name="data">the byte array to send to the peer</param>
        /// <param name="peerId">the Id of the peer we want to send to</param>
        public virtual void SendReliableByteStream(byte[] data, ushort peerId){}
        /// <summary>
        /// Kick a client from the server and send some info with the disconnecting message
        /// </summary>
        /// <param name="clientId">Id of the peer</param>
        /// <param name="customInfoId">custom ID / Header</param>
        /// <param name="reasonInfo">Custom reason string</param>
        /// <param name="unixCustomTimestamp">Custom timestamp </param>
        public abstract void KickClient(ushort clientId, ushort customInfoId, string reasonInfo, long unixCustomTimestamp);
        
        
        /// <summary>
        /// Set configurations for the builtin basic Spam/DoS protection.
        /// Protection is disabled by default. Ensure you configure this to fit your games' normal behavior in order to avoid false detections.
        /// </summary>
        /// <param name="options"></param>
        public virtual void SetAntiSpamConfig(AntiSpamOptions options){}

  
          /// <summary>
    /// Register all message handlers
    /// </summary>
    /// <exception cref="RegisterMethodHandlerException"></exception>
    private void RegisterHandlers()
    {
        MessageHandlers.Clear();
        PnLog.Log($"[Server]: Registering Handlers for Lane: "+Lane);

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            foreach (var type in assembly.GetTypes())
            {
                foreach (var method in type.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    // try
                    // {
                    MessageHandlerAttribute attribute = method.GetCustomAttribute<MessageHandlerAttribute>();
                    if (attribute != null)
                    {
                        var args = method.GetParameters();
                        if (args.Length != 2)
                        {
                           // PnLog.Log($"[Server] Handler Arg length: {args.Length} for method: {method.Name}");
                            continue;
                        }
                        if (attribute.LaneID != Lane)  continue;
                    
                        if (args[0].ParameterType != typeof(ushort) || args[1].ParameterType != typeof(Message))
                        {
                            PnLog.LogException(new RegisterMethodHandlerException($"Failed to register message handlers: Incorrect parameters! Please make sure you have Message and ushort in the right order as arguments for Server sided messageHandlers"));
                            throw new RegisterMethodHandlerException($"Failed to register message handlers: Incorrect signature! Please make sure you have Message and ushort in the right order as arguments for Server sided messageHandlers");
                        }
                        
                        
                        Delegate actionDelegate = Delegate.CreateDelegate(typeof(Action<ushort, Message>), method);
                        if (actionDelegate != null)
                        {
                            Action<ushort, Message> action = (Action<ushort, Message>)actionDelegate;
                            if (!MessageHandlers.TryAdd(attribute.MessageID, action))
                            {
                                PnLog.LogException(new RegisterMethodHandlerException($"Failed to register message handlers: Duplicate key  Key {attribute.MessageID} Handler {method.Name} other key method {MessageHandlers[attribute.MessageID].Method.Name}. Are you trying to register the same MessageHandler ID multiple times?"));
                                throw new RegisterMethodHandlerException($"Failed to register message handlers: Duplicate key. Are you trying to register the same MessageHandler ID multiple times?");
                            }
                        }
                    }
                    // }
                    // catch (Exception e)
                    // {
                    //     throw new RegisterMethodHandlerException($"Failed to register message handlers: {e}");
                    // }
                }
            }
        }
        
        PnLog.Log($"[Server]: Registering {MessageHandlers.Count} Handler methods for lane: {Lane}");
    }

    }
}

