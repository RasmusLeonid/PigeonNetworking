namespace PigeonNetworking.Core
{
    internal struct LogToMainThreadJob 
    {
        internal readonly string Message;
        internal readonly ELogType Type;
    

        internal LogToMainThreadJob(string message, ELogType type)
        {
            Message = message;
            Type = type;
        }

        internal void Execute()
        {
            PnLog.LogOnMainThread(Type, Message);
        }
    }
}


