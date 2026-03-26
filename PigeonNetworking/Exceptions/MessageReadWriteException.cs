using System;

namespace PigeonNetworking.Exceptions
{
    public class MessageReadWriteException : Exception
    {
        public MessageReadWriteException(string message) :  base(message) {}
    }
}

