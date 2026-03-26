using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace PigeonNetworking.Core
{
    
    public enum ELogType
    {
        Info,
        Debug,
        Warning,
        Error,
        Exception,
    }
    public static class PnLog
    {
        
        private static string TimeStamp()
        {
            return "[" + DateTime.Now.ToString("hh:mm:ss") + "]";
        }
        
        private const string LogPrefix = "[PigeonNetworking] ";
        
        public delegate void LogMethod(string log);

        private static Dictionary<ELogType, LogMethod> loggingMethods = new Dictionary<ELogType, LogMethod>();
        private static bool _includeTimeStamp = true;
        private static bool _writeToSystem_Console = false;

        
        /// <summary>
        /// Register logging methods. Only do this once, this will handle logging for all client/server instances
        /// LogMethods can be set to null, if logging for the specific channel is unwanted.
        /// It is highly recommended to not log debugging information, unless actually debugging the netcode.
        /// </summary>
        /// <param name="logInfoMethod">Information</param>
        /// <param name="logDebugMethod">Debugging information, set this to null, if you are not debugging and want to avoid log spam</param>
        /// <param name="logWarningMethod">Log a Warnings</param>
        /// <param name="logErrorMethod">Log an Error</param>
        /// <param name="logExceptionMethod">Log an Exception</param>
        /// <param name="includeTimeStamps">puts a timestamp in front of the log message if true</param>
        public static void RegisterLogMethods(LogMethod logInfoMethod, LogMethod logDebugMethod,
            LogMethod logWarningMethod, LogMethod logErrorMethod, LogMethod logExceptionMethod,  bool includeTimeStamps = true)
        {
            if(logInfoMethod != null)
                loggingMethods.Add(ELogType.Info, logInfoMethod);
            if(logDebugMethod != null)
                loggingMethods.Add(ELogType.Debug, logDebugMethod);
            if(logWarningMethod != null)
                loggingMethods.Add(ELogType.Warning, logWarningMethod);
            if(logErrorMethod != null)
                loggingMethods.Add(ELogType.Error, logErrorMethod);
            if(logExceptionMethod != null)
                loggingMethods.Add(ELogType.Exception, logExceptionMethod);
            
            _includeTimeStamp = includeTimeStamps;

        }

        
        /// <summary>
        /// Specify if we should log to system.Console internally
        /// Default value is false
        /// </summary>
        /// <param name="value"></param>
        public static void SetWriteToSystemConsole(bool value) => _writeToSystem_Console = value;
        
        
        public static void Log(string message, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message, ELogType.Info);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Info, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[LOG]: " + message);
                }
            }
            WriteColoredLine(message, ConsoleColor.Green);
        }
    
        public static void DebugInfo(string message, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message, ELogType.Debug);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Debug, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[Info]: " + message);
                }
            }
            
            WriteColoredLine(message, ConsoleColor.Cyan);
        }

        public static void LogException(Exception exception, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(exception.Message, ELogType.Exception);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Exception, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[EXCEPTION]: " + exception.Message);
                }
            }
            WriteColoredLine(exception.Message, ConsoleColor.Red);
        }
        public static void LogException(string message, Exception exception, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message + exception.Message + exception.StackTrace, ELogType.Exception);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Exception, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[EXCEPTION]: " + message + exception.Message);
                }
            }
            
            WriteColoredLine(message + " Exception:"+ exception.Message + exception.StackTrace, ConsoleColor.Red);
        }
        public static void LogWarning(string message, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message, ELogType.Warning);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Warning, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[WARNING]: " + message);
                }
            }
            
            WriteColoredLine(message, ConsoleColor.Yellow);
        }

        public static void LogDebug(string message, bool threadSafeLog = false)
        {
            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message, ELogType.Debug);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Debug, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[DEBUG]: " + message);
                }
            }

            
            WriteColoredLine(message, ConsoleColor.Blue);
        }

        public static void LogError(string message, bool threadSafeLog = false)
        {

            if (threadSafeLog)
            {
                var job = new LogToMainThreadJob(message, ELogType.Error);
                ThreadManager.DispatchLogJobToMainThread(job);
            }
            else
            {
                if (loggingMethods.TryGetValue(ELogType.Error, out var logMethod))
                {
                    logMethod.Invoke(TimeStamp()+LogPrefix + "[ERROR]: " + message);
                }
            }
            WriteColoredLine(message, ConsoleColor.Red);
        }
        
        /// <summary>
        /// Write a colored line to System.Console
        /// </summary>
        /// <param name="message"></param>
        /// <param name="color"></param>
        public static void WriteColoredLine(string message, ConsoleColor color)
        {
            if(!_writeToSystem_Console) return;
            
            Console.ForegroundColor = color;
            Console.WriteLine(LogPrefix + message);
            Console.ResetColor();
        }

        
        /// <summary>
        /// To be called from Thread job
        /// </summary>
        /// <param name="type"></param>
        /// <param name="message"></param>
        internal static void LogOnMainThread(ELogType type, string message)
        {
            if (loggingMethods.TryGetValue(type, out var logMethod))
            {
                logMethod.Invoke(TimeStamp() + LogPrefix  + $"[{type}]: " + message);
            }
        }
    }
    
    
}

