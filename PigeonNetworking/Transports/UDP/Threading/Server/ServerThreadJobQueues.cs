using System.Collections.Concurrent;
using PigeonNetworking.Core;

namespace PigeonNetworking.Transports.UDP.Threading.Server
{
    public class ServerThreadJobQueues
    {
        private static ConcurrentQueue<ReceiveToMainThreadOnServerJob> sendReceiveJobToMainThreadQueue =
            new ConcurrentQueue<ReceiveToMainThreadOnServerJob>();
    
        internal ConcurrentQueue<SendJob> sendJobsToServerThreadQueue = new ConcurrentQueue<SendJob>();
    
        private ConcurrentQueue<SendToAllJob> sendToAllJobsToServerThreadQueue = new ConcurrentQueue<SendToAllJob>();
    
        private ConcurrentQueue<SendToAllExceptJob> sendToAllExceptJobsToServerThreadQueue = new ConcurrentQueue<SendToAllExceptJob>();


        internal static void DispatchReceiveJobToMainThread(ReceiveToMainThreadOnServerJob job)
        {
            sendReceiveJobToMainThreadQueue.Enqueue(job);
        }

        internal void DispatchSendToNetworkThread(SendJob job)
        {
            sendJobsToServerThreadQueue.Enqueue(job);
        }

        internal void DispatchSendToAllJobToNetworkThread(SendToAllJob job)
        {
            sendToAllJobsToServerThreadQueue.Enqueue(job);
        }

        internal void DispatchToAllExceptJobToNetworkThread(SendToAllExceptJob job)
        {
            sendToAllExceptJobsToServerThreadQueue.Enqueue(job);
        }

        internal static void ExecuteOnMainThread()
        {
            while (sendReceiveJobToMainThreadQueue.TryDequeue(out var job))
            {
                job.Execute();
            }
        }

        internal void ExecuteAllNetworkThreadJobs()
        {
            const int maxJobsPerQueue = 50;
            int counter = 0;
            while (counter < maxJobsPerQueue && sendJobsToServerThreadQueue.TryDequeue(out var job))
            {
                job.Execute();
                counter++;
            }
            counter = 0;
            while (counter < maxJobsPerQueue && sendToAllJobsToServerThreadQueue.TryDequeue(out var job))
            {
                job.Execute();
                counter++;
            }
            counter = 0;
            while (counter < maxJobsPerQueue && sendToAllExceptJobsToServerThreadQueue.TryDequeue(out var job))
            {
                job.Execute();
                counter++;
            }
            
            if(sendJobsToServerThreadQueue.Count > 40) PnLog.LogWarning($"More then 40 jobs detection in sendJobsToServerThreadQueue after tick." +
                                                                        $"Max executions per queue per tick: {maxJobsPerQueue}. QueueSize: {sendJobsToServerThreadQueue.Count}. If this warning keeps appearing, it might suggest a thread starvation or too many jobs being dispatched to the network thread.");
            if(sendToAllJobsToServerThreadQueue.Count > 40) PnLog.LogWarning($"More then 40 jobs detection in sendToAllJobsToServerThreadQueue after tick." +
                                                                             $"Max executions per queue per tick: {maxJobsPerQueue}. QueueSize: {sendToAllJobsToServerThreadQueue.Count}. If this warning keeps appearing, it might suggest a thread starvation or too many jobs being dispatched to the network thread.");
            
            if(sendToAllExceptJobsToServerThreadQueue.Count > 40) PnLog.LogWarning($"More then 40 jobs detection in sendToAllExceptJobsToServerThreadQueue after tick." +
                                                                             $"Max executions per queue per tick: {maxJobsPerQueue}. QueueSize: {sendToAllExceptJobsToServerThreadQueue.Count}. If this warning keeps appearing, it might suggest a thread starvation or too many jobs being dispatched to the network thread.");
        }

    } 
}