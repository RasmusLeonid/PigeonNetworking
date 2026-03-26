using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace PigeonNetworking.Core
{
    public static class NativeMethods
    { 
        //Windows, why are you the way that you are?
//  #if WINDOWS || UNITY_STANDALONE_WIN
//     [DllImport("NativeThreadSleep.dll", CallingConvention = CallingConvention.Cdecl)]
//     public static extern void YieldFor1ms();
// #else
        public static void YieldFor1ms() => Thread.Sleep(1); // Fallback
//#endif



        /// <summary>
        /// Ensure that windows does not default to a max tick rate of 66Hz
        /// Call ClearHighTickRate() high tick rate, before closing the application
        /// </summary>
        public static void EnsureHighTickRate()
        {
           // if (OperatingSystem.IsWindows())
           // {
                TimeBeginPeriod(1);
           // }
        }


        /// <summary>
        /// Not fucking working with unity IL2CPP...
        /// Do this to prevent Windows from decreasing tick rate, when running in background.
        /// Should not be necesarry for clients, but might make sense for dedicated servers or background processes
        /// </summary>
        /// <param name="priority"></param>
        public static void SetAppPriortiy(ProcessPriorityClass priority)
        {
            Process.GetCurrentProcess().PriorityClass = priority;
        }

        public static void ClearHighTickRate()
        {
          //  if (OperatingSystem.IsWindows())
           // {
                TimeEndPeriod(1);
           // }
        }

        [DllImport("winmm.dll", EntryPoint = "timeBeginPeriod", ExactSpelling = true)]
        private static extern int TimeBeginPeriod(uint uPeriod);

        [DllImport("winmm.dll", EntryPoint = "timeEndPeriod", ExactSpelling = true)]
        private static extern int TimeEndPeriod(uint uPeriod);

 
    }
}

