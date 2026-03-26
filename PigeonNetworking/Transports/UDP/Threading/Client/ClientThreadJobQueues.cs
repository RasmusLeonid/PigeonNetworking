using System.Collections.Concurrent;
using PigeonNetworking.Core;

namespace PigeonNetworking.Transports.UDP.Threading.Client
{
    public class ClientThreadJobQueues
    {
        internal ConcurrentQueue<SendJob> sendToNetworkThreadQueue = new ConcurrentQueue<SendJob>();

        private static ConcurrentQueue<DispatchToMainOnClientReceiveJob> sendToMainThreadReceiveQueue =
            new ConcurrentQueue<DispatchToMainOnClientReceiveJob>();


        internal void DispatchSendJobToNetworkThread(SendJob job)
        {
            sendToNetworkThreadQueue.Enqueue(job);
        }

        internal void ExecuteAllNetworkThreadJobs()
        {
            const int maxJobsPerTick = 50;
            int counter = 0;
            while (counter < maxJobsPerTick && sendToNetworkThreadQueue.TryDequeue(out var job))
            {
                job.Execute();
                counter++;
            }

            if (sendToNetworkThreadQueue.Count > 40)
            {
                PnLog.LogWarning($"[ClientThreadJobQueues]: SendToNetworkThreadQueue is bigger then 40, max executions per network tick: {maxJobsPerTick}," +
                                 $" queueSize: {sendToNetworkThreadQueue.Count}. If this warning keeps appearing, it might suggest a thread starvation or too many jobs being dispatched to the network thread");
            }
        }

        internal static void DispatchReceiveToMainThread(DispatchToMainOnClientReceiveJob job)
        {
            sendToMainThreadReceiveQueue.Enqueue(job);
        }

        internal static void ExecuteOnMainThread()
        {
            while (sendToMainThreadReceiveQueue.TryDequeue(out var job))
            {
                job.Execute();
            }
        }
    }
}

