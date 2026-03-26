using System;

namespace PigeonNetworking.Exceptions
{
   
    public class InvalidPortException : Exception
    {
        public  InvalidPortException(string message) : base(message) {}
    }
}
