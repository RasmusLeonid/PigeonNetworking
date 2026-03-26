namespace PigeonNetworking.Core
{
    /// <summary>
    /// Configuration options for builtin DoS/Spam protection.
    /// </summary>
    public class AntiSpamOptions
    {
        /// <summary>
        /// Enable or disable AntiSpam/DoS protection here
        /// </summary>
        public bool ProtectionEnabled = false;
    
        /// <summary>
        ///  default is 10, a higher value is recommended on lower tick rates
        /// </summary>
        public int MaxAllowedMessagesPerTick = 10;

        /// <summary>
        /// default is 10. If a peer exceeds MaxAllowedMessagesPerTick over the entire tracking window, we will kick and block the connection temporarily 
        /// </summary>
        public int TrackingWindowSize = 10;

        /// <summary>
        /// the block duration, when abuse is detected
        /// </summary>
        public int TemporaryBlockDuration = 20000;

        /// <summary>
        /// Temp blocks, before a permanent block is issued, and we escalate to firewall, if configured
        /// </summary>
        public int TempBlocksBeforeEscalation = 10;

        /// <summary>
        /// Is the server allowed to block a client on the OS firewall?
        /// Note: this requires the OS Firewall helper plugin.
        /// </summary>
        public bool AllowOsFirewallEscalation = false;

    }
}


