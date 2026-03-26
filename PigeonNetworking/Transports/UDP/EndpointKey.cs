using System;
using System.Net;

namespace PigeonNetworking.Transports.UDP
{
    public readonly struct EndpointKey : IEquatable<EndpointKey>
    {
        public readonly IPAddress Address;
        public readonly int Port;

        public EndpointKey(IPEndPoint endpoint)
        {
            Address = endpoint.Address.MapToIPv6(); // Normalize to IPv6
            Port = endpoint.Port;
        }

        public bool Equals(EndpointKey other)
        {
            return Port == other.Port && Address.Equals(other.Address);
        }

        public override bool Equals(object obj) => obj is EndpointKey other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 31 + Address.GetHashCode();
                hash = hash * 31 + Port;
                return hash;
            }
        }

        public override string ToString() => $"[{Address}]:{Port}";
    }
}