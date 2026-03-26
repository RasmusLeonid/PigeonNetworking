namespace PigeonNetworking.Core.Enums
{
    // server sided enum
    public enum EDisconnectionReason : ushort
    {
        None,
        Timeout,
        NoAcksReceived,
        LocalNoAcksReceived,
        ServerShutdown,
        Intentional, // initiated by the user
        Kicked,
        LocalTimeout,
        CustomKick,
        TransportError,
        RemotePeerReset,
        ServerAtCapacity,
        FailedToConnect,
        AbuseDetectedByTransport,
    } 
}


