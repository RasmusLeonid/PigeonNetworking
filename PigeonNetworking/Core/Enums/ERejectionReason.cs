namespace PigeonNetworking.Core.Enums
{
    public enum ERejectionReason : ushort
    {
        None = 0,
        Accepted = 1,
        ServerAtCapacity,
        ErrorAccepting,
        FailedToConnect,
    }
}

