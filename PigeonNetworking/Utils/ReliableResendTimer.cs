using System.Diagnostics;

namespace PigeonNetworking.Helpers
{
    public static class ReliableResendTimer
    {
    
        /// <summary>Current timestamp in Stopwatch ticks.</summary>
        public static long Now => Stopwatch.GetTimestamp();

        /// <summary>Converts elapsed Stopwatch ticks to milliseconds.</summary>
        public static double ElapsedMs(long since)
            => (Now - since) * 1000.0 / Stopwatch.Frequency;
    
        /// <summary>Converts milliseconds to Stopwatch ticks.</summary>
        public static long MsToTicks(double ms)
            => (long)(ms * Stopwatch.Frequency / 1000.0);
    }
}

