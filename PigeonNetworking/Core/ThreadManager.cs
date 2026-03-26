using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using PigeonNetworking.Transports.UDP.Threading.Client;
using PigeonNetworking.Transports.UDP.Threading.Server;

namespace PigeonNetworking.Core
{
    public class ThreadManager
    {
        private ServerThreadJobQueues serverThreadJobQueues = new ServerThreadJobQueues();
        private ClientThreadJobQueues clientThreadJobQueues = new ClientThreadJobQueues();
        
        private static  ConcurrentQueue<Action> executeOnMainThreadQueue = new ConcurrentQueue<Action>();

        private static ConcurrentQueue<LogToMainThreadJob> logToMainThreadJobQueue =
            new ConcurrentQueue<LogToMainThreadJob>();

        internal static int MaxActionsPerFrame = 500;
   

        internal ConcurrentQueue<Action> executeOnNetworkThreadQueue = new ConcurrentQueue<Action>();


        internal ServerThreadJobQueues ServerThreadJobsDispatcher => serverThreadJobQueues;
        internal ClientThreadJobQueues ClientThreadJobDispatcher => clientThreadJobQueues;
        
        /// <summary>
        /// Send a log job from any thread to the main thread
        /// </summary>
        /// <param name="logJob"></param>
        internal static void DispatchLogJobToMainThread(LogToMainThreadJob logJob)
        {
            logToMainThreadJobQueue.Enqueue(logJob);
        }

        /// <summary>
        /// Dispatch action to network thread
        /// </summary>
        /// <param name="action"></param>
        internal void DispatchToNetworkThread(Action action)
        {
            executeOnNetworkThreadQueue.Enqueue(action);
        }
      
        internal static void DispatchToMainThread(Action action)
        {
            executeOnMainThreadQueue.Enqueue(action);
        }

        internal void UpdateNetworkThread()
        {
            serverThreadJobQueues.ExecuteAllNetworkThreadJobs();
            clientThreadJobQueues.ExecuteAllNetworkThreadJobs();
            while (executeOnNetworkThreadQueue.TryDequeue(out Action action)) //processed++ < MaxActionsPerFrame && 
            {
                action.Invoke();
            }
        }

        internal static void UpdateMainThread()
        {
            int processed = 0; 
            while (executeOnMainThreadQueue.TryDequeue(out Action action)) //processed++ < MaxActionsPerFrame && 
            {
                processed++;
                action.Invoke();
            }

            while (logToMainThreadJobQueue.TryDequeue(out LogToMainThreadJob job))
            {
                job.Execute();
            }
            ServerThreadJobQueues.ExecuteOnMainThread();
            ClientThreadJobQueues.ExecuteOnMainThread();
        }
    }
}

