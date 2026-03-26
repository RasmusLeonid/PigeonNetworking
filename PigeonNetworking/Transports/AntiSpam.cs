using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using PigeonNetworking.Core;
using PigeonNetworking.Core.Enums;
using PigeonNetworking.Transports.UDP;

namespace PigeonNetworking.Transports
{
    
internal class SpamTracker
{
   private readonly int[] tickBuffer;
   private int windowSize;

   public long BlockedTill = 0;
   public int DetectionCount = 0;
   public bool IsBlocked = false;
   public bool IsPermaBlocked = false;
   public bool HasKickedPeer = false;
   private long lastTick = -1;
   private int currentIndex = -1;

   public SpamTracker(int tickWindow)
   {
      windowSize = tickWindow;
      tickBuffer = new int[tickWindow];
   
   }

   public void RecordMessage(long tickId)
   {
      if (tickId < lastTick)
      {
         // reset window
         lastTick = -1;
         currentIndex = -1;
         return;
      }
      // first message after reset
      if (lastTick == -1) 
      {
         currentIndex = 0;
         tickBuffer[currentIndex] = 1;
         lastTick = tickId;
         return;
      }

      if (tickId == lastTick)
      {
         tickBuffer[currentIndex]++;
      }
      else if (tickId > lastTick)
      {
         // New tick — move index forward
         currentIndex = (currentIndex + 1) % windowSize;
         tickBuffer[currentIndex] = 1;
         lastTick = tickId;
      }
   }

   public int GetSpammedTicks(int spamThreshold)
   {
      int count = 0;
      for (int i = 0; i < tickBuffer.Length; i++)
      {
         if (tickBuffer[i] >= spamThreshold)
         {
            count++;
         }
      }
      return count;
   }
}
internal class AntiSpam
{
   private readonly Dictionary<IPEndPoint, SpamTracker> _trackers = new();
   private readonly UdpServer _server;

   private bool _allowProtection = false;
   private int _tickWindow;
   private int _messagesPerTickThreshold;
   private int _detectionCounTillFirewallEscalation = 10;
   private int _tempBlockDuration = 20000;
   private int _detectionsBeforeEscalation = 10;
   private bool _allowEscalationToOsFirewall = true;

   private long _currentTick = 0;

   public AntiSpam(UdpServer server, AntiSpamOptions options)
   {
      _server = server;
      _tickWindow = options.TrackingWindowSize;
      _messagesPerTickThreshold = options.MaxAllowedMessagesPerTick;
      _tempBlockDuration = options.TemporaryBlockDuration;
      _detectionsBeforeEscalation = options.TempBlocksBeforeEscalation;
      _allowEscalationToOsFirewall = options.AllowOsFirewallEscalation;
      _allowProtection = options.ProtectionEnabled;
   }

   public void Tick()
   {
      _currentTick++;
      
   }
   public void SetConfig(AntiSpamOptions options)
   {
      
      _tickWindow = options.TrackingWindowSize;
      _messagesPerTickThreshold = options.MaxAllowedMessagesPerTick;
      _tempBlockDuration = options.TemporaryBlockDuration;
      _detectionsBeforeEscalation = options.TempBlocksBeforeEscalation;
      _allowEscalationToOsFirewall = options.AllowOsFirewallEscalation;
      _allowProtection = options.ProtectionEnabled;
      _trackers.Clear();
      
   }
   public bool IsBlocked(IPEndPoint ip)
   {
      if (!_allowProtection) return false;
      
      RegisterMessage(ip);
      if (_trackers.TryGetValue(ip, out var tracker) && tracker.IsBlocked)
      {
         if (tracker.IsPermaBlocked) return true;
         if (tracker.BlockedTill < Stopwatch.GetTimestamp())
         {
            tracker.IsBlocked = false;
            tracker.HasKickedPeer = false;
            tracker.IsBlocked = false;
            return false;
         }
         return true;
      }

      return false;
   }

   public void RegisterMessage(IPEndPoint ip)
   {
      if(!_allowProtection) return;
      
      if (!_trackers.TryGetValue(ip, out var tracker))
      {
         tracker = new SpamTracker(_tickWindow);
         _trackers[ip] = tracker;
      }

      if (tracker.IsBlocked)
         return;

      tracker.RecordMessage(_currentTick);

      int spamTicks = tracker.GetSpammedTicks(_messagesPerTickThreshold);

      if (spamTicks >= _tickWindow)
      {
         if (!tracker.IsBlocked)
         {
            tracker.DetectionCount++;
            tracker.IsBlocked = true;
            long banDurationTicks = Stopwatch.Frequency * 20;
            tracker.BlockedTill =  Stopwatch.GetTimestamp() + banDurationTicks; // ban for 20 seconds
            if (!tracker.HasKickedPeer && _server._clientRegistry.TryGetByEndpoint(ip, out var peer))
            {
               tracker.HasKickedPeer = true;
               _server.KickPeer(peer, EDisconnectionReason.AbuseDetectedByTransport);
            }

            if (tracker.DetectionCount >= _detectionCounTillFirewallEscalation)
            {
               EscalateToOSFirewall(ip, tracker);
            }
         }
         PnLog.LogWarning($"[DoS Protection] Temporary blocked endpoint {ip} for excessive message spam.");
      }
   }

   private void EscalateToOSFirewall(IPEndPoint ip, SpamTracker tracker)
   {
      PnLog.LogWarning($"[DoS Protection] Endpoint {ip} has received multiple blocks in a row. Perma blocking and trying to escalate to OS firewall.");
      tracker.IsPermaBlocked = true;
      //todo: add callback to game thread, try to block IP on OS firewall through helper program
   }
 
}
    
}

