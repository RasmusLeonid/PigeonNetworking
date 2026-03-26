using System;
using System.Collections.Concurrent;
using System.Reflection;
using PigeonNetworking.Client.ClientEvents;
using PigeonNetworking.Core;
using PigeonNetworking.Exceptions;

namespace PigeonNetworking.Client
{

public abstract class Client : BaseNetEndpoint
{
    
    internal ConcurrentDictionary<ushort, Action<Message>> MessageHandlers = new ConcurrentDictionary<ushort, Action<Message>>();
    protected bool _isConnected = false;
    internal Peer serverPeer = new Peer();
    protected ushort _id = 0;

    
    /// <summary>
    /// Get the Id of our client, assigned from the server
    /// </summary>
    public ushort Id => _id;

    
    /// <summary>
    /// Initialize the client object
    /// </summary>
    /// <param name="laneId">Setting a lane will allow you to run multiple independent client instances, receiving on the specified lane only.
    /// default lane is 0 and does not have to be specified</param>
    public Client(byte laneId)
    {
        Lane = laneId;
        RegisterHandlers();
        PnLog.Log("Starting client");
    }
    /// <summary>
    /// Invoked when the client successfully connected to the server and transport is ready
    /// </summary>
    public EventHandler<ConnectedToServerEvent> OnConnectedToServer;
    /// <summary>
    /// Invoked when the connection was rejected by the server, or if we failed to connect
    /// </summary>
    public EventHandler<ConnectionRejectedFromServerEvent> OnFailedToConnect;
    /// <summary>
    /// Invoked when the client disconnected from the server and transport stopped
    /// </summary>
    public EventHandler<DisconnectedFromServerEvent> OnDisconnectedFromServer;

    /// <summary>
    /// Invoked when another client connected to the server
    /// </summary>
    public EventHandler<OtherClientConnectedEvent> OnOtherClientConnected;
    /// <summary>
    /// Invoked when another client disconnected to the server
    /// </summary>
    public EventHandler<OtherClientDisconnectedEvent> OnOtherClientDisconnected;
    
    
    /// <summary>
    /// Invoked when a large reliable byte stream has been fully received. Sent by calling SendReliableByteStream on remote Peer
    /// </summary>
    public EventHandler<LargeByteStreamReceivedEvent> OnLargeByteStreamReceived;
   
    
    
    /// <summary>
    /// Call this every tick
    /// Used to execute threadManager events on UDP Transport and to run callbacks on SDR
    /// </summary>
    public virtual void Tick(){}
    
    
    
    /// <summary>
    /// Start the Client
    /// </summary>
    public virtual void Start()
    {
  
    }
    
    
    /// <summary>
    /// Get the tick rate (Frames per second) of the Network thread loop. This will only be set on multithreaded transports.
    /// Tick rate is set to 1000 Hz by default.
    /// Intended for debugging
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
    /// Stop the client
    /// </summary>
    public virtual void Stop()
    {
       
    }
    
    /// <summary>
    /// Get the RTT in milliseconds from the active transport
    /// </summary>
    public int GetRTT => serverPeer.RTT;
    
    /// <summary>
    /// Get RTT in milliseconds based on heartbeats, will only be set if enabled on the server (depending on transport)
    /// </summary>
    public int GetHeartbeatRTT => serverPeer.HeartbeatRtt;
   
 
    /// <summary>
    /// returns true if we are connected to a server, otherwise false
    /// </summary>
    /// <returns>bool</returns>
    public abstract bool IsConnected();

    /// <summary>
    /// Connect to a server
    /// </summary>
    /// <param name="socketInfo"></param>
    public abstract void Connect(string connectInfo);
    
    
    /// <summary>
    /// Send a message to the server
    /// </summary>
    /// <param name="message"></param>
    public virtual void Send(Message message)
    {
       
    }

    
    /// <summary>
    /// only use this, if you need to send a byte array that is bigger than the max message size.
    /// This function should not be used too frequently and is intended to handle edge cases, where
    /// the data we need to send does not fit into the Message class' buffer/MTU.
    /// This sends a large byte array, by splitting it into as many segments as needed for transport and
    /// ensures all chunks are received and ordered, before the buffer is dispatched with an OnLargeByteStreamReceived event.
    /// When using this function, you will need to manage handling the data yourself, by e.g. writing your own header
    /// into the byte array, before sending it.
    /// 
    /// </summary>
    /// <param name="data">the byte array to send to the server</param>
    public virtual void SendReliableByteStream(byte[] data)
    {
        
    }
    /// <summary>
    /// Disconnect from the server
    /// </summary>
    public virtual void Disconnect()
    {
        
    }

    
    /// <summary>
    /// Registers all message handler methods with the given lane ID
    /// </summary>
    /// <exception cref="RegisterMethodHandlerException"></exception>
    private void RegisterHandlers()
    {
        MessageHandlers.Clear();
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
              foreach (var type in assembly.GetTypes())
              {
                  foreach (var method in type.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
                  {
                      try
                      {
                          MessageHandlerAttribute attribute = method.GetCustomAttribute<MessageHandlerAttribute>();
                          if (attribute != null)
                          {
                              if(attribute.LaneID != Lane) continue;
                              var args = method.GetParameters();
                              
                              if (args.Length != 1) continue;
                              if (args[0].ParameterType != typeof(Message))
                              {
                                  PnLog.LogException(new RegisterMethodHandlerException($"Failed to register message handlers: Incorrect parameters! Please make sure you have Message and ushort in the right order as arguments for Server sided messageHandlers"));
                                  throw new RegisterMethodHandlerException($"Failed to register message handlers: Incorrect signature! Please make sure you have Message and ushort in the right order as arguments for Server sided messageHandlers");
                              }
                        
                              Delegate actionDelegate = Delegate.CreateDelegate(typeof(Action<Message>), method);
                              if (actionDelegate != null)
                              {
                                  Action<Message> action = (Action<Message>)actionDelegate;
                                  if (!MessageHandlers.TryAdd(attribute.MessageID, action))
                                  {
                                      PnLog.LogException(new RegisterMethodHandlerException($"Failed to register message handlers: Duplicate key. Are you trying to register the same MessageHandler ID multiple times?"));
                                      throw new RegisterMethodHandlerException($"Failed to register message handlers: Duplicate key. Are you trying to register the same MessageHandler ID multiple times?");
                                  }
                                  PnLog.LogDebug($"Registered Handler message with key: {attribute.MessageID} - Handler: {method.Name}");
                              }
                          }
                      }
                      catch (Exception e)
                      {
                          throw new RegisterMethodHandlerException($"Failed to register message handlers: {e}");
                      }
                  }
              }
        }
        PnLog.LogDebug($"[Client] Registered {MessageHandlers.Count} Message Handlers for Lane: {Lane}");
    }


  
}
}
