using System;

namespace PigeonNetworking.Exceptions
{
    public class InvalidIpPortStringException :  Exception
    {
        public InvalidIpPortStringException(string message) : base(message) {}
  
    }
}

