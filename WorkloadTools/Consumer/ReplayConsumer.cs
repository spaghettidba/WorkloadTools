using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using WorkloadTools.Consumer.Replay;

namespace WorkloadTools.Consumer
{
    public class ReplayConsumer : BufferedWorkloadConsumer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private const int SEMAPHORE_LIMIT = 32;
        private static readonly Semaphore WorkLimiter = new Semaphore(SEMAPHORE_LIMIT, SEMAPHORE_LIMIT);

        public SqlConnectionInfo ConnectionInfo { get; set; }

        private ConcurrentDictionary<int, ReplayWorker> ReplayWorkers = new ConcurrentDictionary<int, ReplayWorker>();
        private bool stopped = false;
        private Thread runner;


        public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if (evt.Type != WorkloadEvent.EventType.RPCCompleted && evt.Type != WorkloadEvent.EventType.BatchCompleted)
                return;

            ReplayCommand command = new ReplayCommand()
            {
                CommandText = evt.Text,
                Database = evt.DatabaseName
            };

            int session_id = -1;
            session_id = evt.SPID;

            ReplayWorker rw = null;
            if (ReplayWorkers.TryGetValue(session_id, out rw))
            {
                rw.AppendCommand(command);
            }
            else
            {
                rw = new ReplayWorker()
                {
                    ConnectionInfo = this.ConnectionInfo,
                    ReplayIntervalSeconds = 0,
                    StopOnError = false,
                    Name = session_id.ToString()
                };
                ReplayWorkers.TryAdd(session_id, rw);
                rw.AppendCommand(command);

                logger.Info(String.Format("Started new Replay Worker for session_id {0}", session_id));
            }

            if(runner == null)
            {

                runner = new Thread(new ThreadStart(
                                delegate
                                {
                                    try
                                    {
                                        RunWorkers();
                                    }
                                    catch (Exception e)
                                    {
                                        try { logger.Error(e, "Unhandled exception in TraceManager.RunWorkers"); }
                                        catch { Console.WriteLine(e.Message); }
                                    }
                                }
                                ));
                runner.IsBackground = true;
                runner.Start();
            }

        }


        protected override void Dispose(bool disposing)
        {
            base.Dispose();
            stopped = true;
        }


        private void RunWorkers()
        {
            while (!stopped)
            {
                int cnt = 0;
                lock (ReplayWorkers)
                {
                    cnt = ReplayWorkers.Count;
                }
                if (cnt == 0)
                {
                    Thread.Sleep(100);
                    continue;
                }


                try
                {
                    lock (ReplayWorkers)
                    {
                        foreach (ReplayWorker wrk in ReplayWorkers.Values)
                        {
                            if (stopped) return;

                            if (wrk.CommandCount > 0)
                            {
                                try
                                {
                                    // Using a semaphore to avoid overwhelming the threadpool
                                    // Without this precaution, the memory consumption goes to the roof
                                    WorkLimiter.WaitOne();

                                    // Queue the execution of a statement in the threadpool.
                                    // The statement will get executed in a separate thread eventually.
                                    ThreadPool.QueueUserWorkItem(delegate { try { wrk.ExecuteNextCommand(); } catch (Exception e) { try { logger.Error(e, "Unhandled exception in ReplayWorker.ExecuteCommand"); } catch { Console.WriteLine(e.Message); } } });
                                }
                                finally
                                {
                                    // Release the semaphore
                                    WorkLimiter.Release();
                                }
                            }

                        }
                    }

                    // Sleep 1 millisecond after every execution of all the statements
                    // queued in the ReplayWorkers. 
                    // This sleep is absolutely necessary to avoid burning CPU when
                    // all the ReplayWorkers do not contain any statements.

                    Thread.Sleep(1);
                }
                catch (InvalidOperationException)
                {
                    //ignore ...
                }

            }

        }


    }
}
