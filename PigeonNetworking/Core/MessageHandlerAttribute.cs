using System;

namespace PigeonNetworking
{
    [AttributeUsage(AttributeTargets.Method)]
    public class MessageHandlerAttribute : Attribute
    {
        public ushort MessageID { get; }
        
        public  byte LaneID { get; }

        
        /// <summary>
        /// Method signature patterns
        /// Client pattern: Message message
        /// Server pattern: ushort peerId, Message message
        /// </summary>
        /// <param name="messageId"></param>
        public MessageHandlerAttribute(ushort messageId)
        {
            MessageID = messageId;
            LaneID = 0;
        }
        
        public MessageHandlerAttribute(ushort messageId, byte laneId)
        {
            MessageID = messageId;
            LaneID = laneId;
        }
    }
}


