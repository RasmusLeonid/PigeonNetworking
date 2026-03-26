# PigeonNetworking

![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)
![Unity](https://img.shields.io/badge/unity-compatible-blue&#41)
![C#](https://img.shields.io/badge/language-C%23-purple)

**PigeonNetworking** is a lightweight C# networking library built for (but not only) **Unity multiplayer games** as an alternative to Riptide. PigeonNetworking is heavily inspired by Riptide and is designed to feel similar to Riptide in usage, but implements a different internal architecture. It supports both **UDP** (with reliable and unreliable channels + message batching) and **Steam Networking (Steam Datagram Relay - SDR)** transports.
Steam and other transports are maintained in separate repositories:
- [Steam Transport for Steamworks.NET](https://github.com/RasmusLeonid/PigeonNetworking.SteamworksSDR)
- [Steam Transport for Facepunch Steamworks](https://github.com/RasmusLeonid/PigeonNetworking.FacepunchSDR)
- EOS Transport is being worked on and will likely be available soon (no guarantee)


Note:
So far I have only briefly tested the Facepunch and EOS transports.

##  Disclaimer

This library is work-in-progress and not fully battle-tested yet. It has been working reliably with UDP and the Steamworks.NET transport in my own unreleased game, [Olympian Knights](https://store.steampowered.com/app/1833860/Olympian_Knights/), but use this library at your own discretion.


## ✨ Features

- 🔌 **UDP Transport** with built-in reliable and unreliable channels, running on a background thread to prevent timeouts caused by main thread lag
- 🔗 **Steam SDR Transport** integration for both, Steamworks.NET and Facepunch Steamworks
- 🧠 Simple API for sending and receiving messages
- 🧊 Send large byte arrays that don't fit inside the Message MTU, with `SendReliableByteStream`. This can be useful for sending images or Steam inventory tickets, which can be very large apparently.
- 🧩 Modular, extensible design
- 🕹️ Unity-friendly architecture, with a usage pattern nearly identical to Riptide
- 📦 Custom message batching system, with adjustable Nagle like timer (Can be set to 0 or disabled, but if you send many small messages, you should leave it on e.g. 0-5ms).
- 🛡️ Basic abuse prevention system to detect and temporarily or permanently block malicious clients, that are sending an excessive amount of messages. (Disabled by default, enable and configure it as needed for your project).
---

## Why PigeonNetworking?
I created PigeonNetworking because I love Riptide, but its single-threaded architecture often caused frustrating timeouts during editor pauses, loading screens, or when tabbing out. PigeonNetworking avoids these issues by using a multithreaded design for its UDP transport and by fully relying on SteamNetworkingSockets’ internal connection logic when using the Steam transport.

### ✅ Pros:
- 🧵 Runs on its own thread — no timeouts when tabbing out or during main thread stalls
- 🧪 Editor-safe: Pause the Unity Editor while connected without being disconnected
- 🧵 Send large byte arrays that exceed MTU, with `SendReliableByteStream` (intended for occasional large payloads; not yet optimized for frequent use).
- 📦 Custom Nagle like batching to reduce bandwidth usage (configurable)
- 🔗 Full SteamNetworkingSockets integration (no redundant logic)
- 🎮 Supports multiple client/server instances simultaneously (even with different transports)
- 🛠️ Efficient: uses object pooling and a struct-based job system to minimize GC pressure
- 🛡️ Helps protect your server from malicious or buggy clients by detecting and blocking excessive message spam (configurable and opt-in)


### ❌ Cons:
- 🧪 Still new and not fully battle-tested
- 📭 No Notify channel (like Riptide’s)
- 🧮 No bit-level serialization (currently considered overkill for target use cases)
- 🤷‍♂️ Possible feature gaps compared to Riptide
- ⏱ Inter-thread communication and tick rate differences add a small delay. However, it should be within ~1-2ms (Unity does this for you, but outside of unity you might want to use TimeBeginPeriod on windows, to allow the IO Thread to tick faster)
- The Message class turned out a bit more messy than I wanted, after making it compatible with multiple transports
- I haven't gotten around to build a DLL that unity can load yet, but the source code works well in unity and outside of unity in plain C#
- So far I have only tested this library with 2-5 players. In theory, this library should be able to handle a lot more, but you might want to do some tests of your own, if you need very large player counts.

**Credits:**

Special thanks to [Tom Weiland](https://github.com/tom-weiland) for developing [RiptideNetworking](https://github.com/RiptideNetworking/Riptide) and giving me the courage to actually develop my own Networking library as well. His tutorials helped me massively when getting started with multiplayer game development and network programming.

-------------------------------------------------------------

## Architecture

PigeonNetworking is designed to be flexible and does not enforce a specific network architecture. You can use it in:
- Dedicated server setups
- Listen-server (client-hosted) setups
- Hybrid approaches combining both

### Unity tip:
In my game Olympian Knights, I use Unity’s Physics Scene feature to run both the client and the server within the same application instance. This allows both dedicated and listen-server setups to share the exact same gameplay codebase, minimizing differences between server types.
#### The way this works:
- Load two instances of the same scene (map) using additive loading
  - One scene acts as the server simulation
  - The other acts as the client simulation
- Start a server and connect to it using a local client within the same application instance
- Remove or disable all renderers in the server scene to keep the server simulation invisible
- Call physics queries (e.g., raycasts) directly on the correct physics scene
    - Using Physics.Raycast will not work correctly in this setup, Instead, use the specific physics scene API
    - Be careful with Singletons as they can be accessed from both scenes

#### Limitations:
Pigeon Networking is probably not well suited for mesh-based P2P as it's designed around a client-Server architecture.

-------------------------------------------------------------

## 🚀 Getting Started

### 🖥️ Create a Server in Unity
```csharp
    private bool SteamInit = false;
    private void StartServer()
    {

        // server = new SteamServer() for steam transport
        server = new UdpServer(); // for UDP Transport
        server.Start(7777);
        server.OnClientConnected += OnPlayerConnected;
        server.OnClientDisconnected += OnPlayerDisconnected;
    }
    
    private void Update()
    {
        server?.Tick();
    }
    
    private void OnPlayerConnected(object sender, ClientConnectedEvent connectedEvent)
    {
        Debug.Log("[Unity] Client connected");
    }
    private void OnPlayerDisconnected(object sender, ClientDisconnectedEvent connectedEvent)
    {
        Debug.Log("[Unity] Client disconnected");
    }

```
### 📱 Create a Client in Unity
```csharp
    [SerializeField] private TMP_InputField _inputField;
    
    public static Client client;
    private void StartClient()
    {
        //client = new SteamClient(); // Steam Transport
        client = new UdpClientManager();
        
        client.OnConnectedToServer += OnConnectedToServer;
        client.OnDisconnectedFromServer += OnDisconnectedFromServer;
        client.OnOtherClientConnected += OnPlayerConnected;
        client.OnOtherClientDisconnected += OnPlayerDisconnected;
        client.OnConnectionRejected += OnConnectionRejectedFromServer;
        client.Start();
        Debug.Log("Client start called");
    }
    private void OnConnectionRejectedFromServer(object sender, ConnectionRejectedFromServerEvent callback)
    {
        Debug.Log($"Connection Rejected from server - Reason {callback.RejectionReason}");
    }
    private void OnConnectedToServer(object sender, ConnectedToServerEvent callback)
    {
        Debug.Log($"Connected to the Server ");
    }
    private void OnDisconnectedFromServer(object sender, DisconnectedFromServerEvent callback)
    {
        Debug.Log($"Disconnected from the Server - reason {callback.Reason}");
    }
    
    private void OnPlayerConnected(object sender, OtherClientConnectedEvent connectedEvent)
    {
        Debug.Log($"Other Player joined - Id: {connectedEvent.ClientId}");
    }
    
    private void OnPlayerDisconnected(object sender, OtherClientDisconnectedEvent connectedEvent)
    {
        Debug.Log($"Other Player left - Id: {connectedEvent.ClientId}");
    }

    public void TryConnect()
    {
        if (ulong.TryParse(_inputField.text, out ulong id))
        {
            CSteamID serverId = new CSteamID(id);
            client.Connect($"{id}:0")); // connect to SDR server <steamID:virtualPort>
        }
        else
        {
            client.Connect(_inputField.text); // or connect UDP server <IPAddress:Port>
        }
    }

    public void Disconnect()
    {
        client.Disconnect();
    }
```

### 📤 Send and Receive a Message
```csharp
	// send message on the server to the client
    private static void GrantPlayerAccess(ushort clientId)
    {
        Message message = Message.Create(EChannel.Reliable, (ushort)EServerToClientPacketId.GrantAccess);
        message.WriteBool(true);
        server.Send(message, clientId);
    }

	// receive message on the client
    [MessageHandler((ushort)EServerToClientPacketId.GrantAccess)]
    private static void HandleAccessGrant(Message rMessage)
    {
        bool granted = rMessage.ReadBool();
        Debug.Log("Granted access by server: "+ granted);

        Message message = Message.Create(EChannel.Unreliable, (ushort)EClientToServerPacketId.TestUnreliable1);
        message.WriteFloat(200.5f); // Add... works too
        message.WriteDouble(25.5d);
        message.WriteSmartString("Hello there!");
        message.WriteByte(20);
        message.WriteInt(50);
        client.Send(message);
    }

    // send message from client to server
    private static void SendStuffToServer()
    {
        Message message = Message.Create(EChannel.Unreliable, (ushort)EClientToServerPacketId.HelloServer);
        message.WriteFloat(200.5f);
        message.WriteDouble(25.5d);
        message.WriteSmartString("Unreliable Test: Big!");
        message.WriteByte(20);
        message.WriteInt(50);
        client.Send(message);
    }

    // handle message on the server
    [MessageHandler((ushort)EServerToClientPacketId.HelloServer)]
    private static void HandleAccessGrant(ushort peerId, Message message)
    {
        float f =  message.ReadFloat(); // Get... works too
        double d = message.ReadDouble();
        string s = message.ReadSmartString();
        byte b = message.ReadByte();
        int i = message.ReadInt();
        Debug.Log($"[Unity] Received unreliable test message, data: f{f}, d{d}, s{s}, b{b}, i{i}");
    }
```

## 🧭 Planned / Possible Features
*(Nothing is guaranteed - these are just ideas)*
- Encrypted channel for UDP transport (Steam already does this by default)
- Better optimized `LargeByteStream` method
- Notify channel

## 📄 License

This project is licensed under the [MIT License](LICENSE).


