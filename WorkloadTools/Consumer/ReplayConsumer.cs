using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer.Replay;

namespace WorkloadTools.Consumer
{
    public class ReplayConsumer : BufferedWorkloadConsumer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private const int SEMAPHORE_LIMIT = 32;
        private const int WORKER_EXPIRY_TIMEOUT_MINUTES = 5;
        private static readonly Semaphore WorkLimiter = new Semaphore(SEMAPHORE_LIMIT, SEMAPHORE_LIMIT);

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public SynchronizationModeEnum SynchronizationMode { get; set; } = SynchronizationModeEnum.None;

        private ConcurrentDictionary<int, ReplayWorker> ReplayWorkers = new ConcurrentDictionary<int, ReplayWorker>();
        private bool stopped = false;
        private Thread runner;
        private Thread sweeper;

        public enum SynchronizationModeEnum
        {
            ThreadPools,
            Tasks,
            None
        }



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
            session_id = (int)evt.SPID;

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


            if (sweeper == null)
            {

                sweeper = new Thread(new ThreadStart(
                                delegate
                                {
                                    try
                                    {
                                        RunSweeper();
                                    }
                                    catch (Exception e)
                                    {
                                        try { logger.Error(e, "Unhandled exception in TraceManager.RunSweeper"); }
                                        catch { Console.WriteLine(e.Message); }
                                    }
                                }
                                ));
                sweeper.IsBackground = true;
                sweeper.Start();
            }

        }


        protected override void Dispose(bool disposing)
        {
            stopped = true;
        }



        // Sweeper thread: removes from the workers list all the workers
        // that have not executed a command in the last 5 minutes
        private void RunSweeper()
        {
            while (!stopped)
            {
                if (ReplayWorkers.IsEmpty)
                {
                    Thread.Sleep(100);
                    continue;
                }

                foreach (ReplayWorker wrk in ReplayWorkers.Values)
                {
                    if (wrk.LastCommandTime < DateTime.Now.AddMinutes(-WORKER_EXPIRY_TIMEOUT_MINUTES))
                    {
                        ReplayWorker outWrk;
                        ReplayWorkers.TryRemove(Int32.Parse(wrk.Name), out outWrk);
                        outWrk.Dispose();
                    }
                }

                Thread.Sleep(10000); // sleep 10 seconds
            }
            logger.Trace("Sweeper thread stopped.");
        }


        private void RunWorkers()
        {
            while (!stopped)
            {
                if (ReplayWorkers.IsEmpty)
                {
                    Thread.Sleep(100);
                    continue;
                }


                try
                {
                    //Parallel.ForEach(ReplayWorkers.Values, (ReplayWorker wrk) =>
                    foreach (ReplayWorker wrk in ReplayWorkers.Values)
                    {
                        if (stopped) return;

                        if (wrk.HasCommands)
                        {

                            if (SynchronizationMode == SynchronizationModeEnum.ThreadPools)
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
                            else if (SynchronizationMode == SynchronizationModeEnum.Tasks)
                            {
                                try
                                {
                                    // Using a semaphore to avoid overwhelming the threadpool
                                    // Without this precaution, the memory consumption goes to the roof
                                    WorkLimiter.WaitOne();

                                    // Start a new Task to run the statement
                                    Task t = Task.Factory.StartNew(delegate { try { wrk.ExecuteNextCommand(); } catch (Exception e) { try { logger.Error(e, "Unhandled exception in ReplayWorker.ExecuteCommand"); } catch { Console.WriteLine(e.Message); } } });
                                }
                                finally
                                {
                                    // Release the semaphore
                                    WorkLimiter.Release();
                                }
                            }
                            else if (SynchronizationMode == SynchronizationModeEnum.None)
                            {
                                try
                                {
                                    if (!wrk.IsRunning)
                                    {
                                        wrk.Start();
                                    }
                                }
                                catch (Exception e)
                                {
                                    logger.Error(e, "Unhandled exception in ReplayWorker.ExecuteCommand");
                                }
                            }
                        }
                    };


                    if (SynchronizationMode == SynchronizationModeEnum.None)
                    {
                        // Sleep 1 second before checking whether more workers
                        // are available and not started
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        // Sleep 1 millisecond after every execution of all the statements
                        // queued in the ReplayWorkers. 
                        // This sleep is absolutely necessary to avoid burning CPU when
                        // all the ReplayWorkers do not contain any statements.
                        Thread.Sleep(1);
                    }
                }
                catch (InvalidOperationException)
                {
                    //ignore ...
                }

            }


            if (SynchronizationMode == SynchronizationModeEnum.None)
            {
                foreach (ReplayWorker wrk in ReplayWorkers.Values)
                {
                    wrk.Stop();
                }
            }
            logger.Trace("Worker thread stopped.");
        }

    }
}
