using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer.Replay;

namespace WorkloadTools.Consumer.Replay
{
    public class ReplayConsumer : BufferedWorkloadConsumer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private SpinWait spin = new SpinWait();
        public int ThreadLimit = 32;
        public int InactiveWorkerTerminationTimeoutSeconds = 300;
        private Semaphore WorkLimiter;

		public bool DisplayWorkerStats { get; set; } = true;
		public bool ConsumeResults { get; set; } = true;
		public int QueryTimeoutSeconds { get; set; } = 30;
		public int WorkerStatsCommandCount { get; set; } = 1000;
		public bool MimicApplicationName { get; set; } = false;

        public Dictionary<string, string> DatabaseMap { get; set; } = new Dictionary<string, string>();

		public SqlConnectionInfo ConnectionInfo { get; set; }
        public SynchronizationModeEnum SynchronizationMode { get; set; } = SynchronizationModeEnum.WorkerTask;

        private ConcurrentDictionary<int, ReplayWorker> ReplayWorkers = new ConcurrentDictionary<int, ReplayWorker>();
        private Thread runner;
        private Thread sweeper;

        private long eventCount;
        private DateTime startTime = DateTime.MinValue;

        // holds the total number of events to replay
        // only available when reading from a file
        // for realtime replays this is not available
        private long totalEventCount = 0;

        public enum SynchronizationModeEnum
        {
            ThreadPools,
            Tasks,
            WorkerTask,
            Serial
        }

        public ReplayConsumer()
        {
            WorkLimiter = new Semaphore(ThreadLimit, ThreadLimit);
        }

        public override void ConsumeBuffered(WorkloadEvent evnt)
        {
            if(evnt is MessageWorkloadEvent)
            {
                MessageWorkloadEvent msgEvent = evnt as MessageWorkloadEvent;
                if (msgEvent.MsgType == MessageWorkloadEvent.MessageType.TotalEvents)
                {
                    try
                    {
                        totalEventCount = (long)msgEvent.Value;
                    }
                    catch (Exception e)
                    {
                        logger.Error($"Unable to set the total number of events: {e.Message}");
                    }
                }
            }

            if (!(evnt is ExecutionWorkloadEvent))
                return;

            if (evnt.Type != WorkloadEvent.EventType.RPCCompleted && evnt.Type != WorkloadEvent.EventType.BatchCompleted)
                return;

            eventCount++;
            if ((eventCount > 0) && (eventCount % WorkerStatsCommandCount == 0))
            {
                string percentInfo = (totalEventCount > 0) ? "( " + ((eventCount * 100) / totalEventCount).ToString() + "% )" : "";
                logger.Info($"{eventCount} events queued for replay {percentInfo}");
            }

            if (startTime == DateTime.MinValue)
            {
                startTime = DateTime.Now;
            }

            ExecutionWorkloadEvent evt = (ExecutionWorkloadEvent)evnt;

            ReplayCommand command = new ReplayCommand()
            {
                CommandText = evt.Text,
                Database = evt.DatabaseName,
                ApplicationName = evt.ApplicationName,
                ReplayOffset = evt.ReplayOffset,
                EventSequence = evt.EventSequence
            };

            int session_id = -1;
            session_id = (int)evt.SPID;

            ReplayWorker rw = null;
            if (ReplayWorkers.TryGetValue(session_id, out rw))
            {
                // Ensure that the buffer does not get too big
                while (rw.QueueLength >= (BufferSize * .9))
                {
                    spin.SpinOnce();
                }
                rw.AppendCommand(command);
            }
            else
            {
                rw = new ReplayWorker()
                {
                    ConnectionInfo = this.ConnectionInfo,
                    ReplayIntervalSeconds = 0,
                    StopOnError = false,
                    Name = session_id.ToString(),
					DisplayWorkerStats = this.DisplayWorkerStats,
					ConsumeResults = this.ConsumeResults,
					QueryTimeoutSeconds = this.QueryTimeoutSeconds,
					WorkerStatsCommandCount = this.WorkerStatsCommandCount,
					MimicApplicationName = this.MimicApplicationName,
                    DatabaseMap = this.DatabaseMap,
                    StartTime = startTime
				};
                ReplayWorkers.TryAdd(session_id, rw);
                rw.AppendCommand(command);

                logger.Info($"Worker [{session_id}] - Starting");
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
                                        try { logger.Error(e, "Unhandled exception in ReplayConsumer.RunWorkers"); }
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
            foreach(var r in ReplayWorkers.Values)
            {
                r.Dispose();
            }
            WorkLimiter.Dispose();
            stopped = true;
        }



        // Sweeper thread: removes from the workers list all the workers
        // that have not executed a command in the last 5 minutes
        private void RunSweeper()
        {
            while (!stopped)
            {
                try
                {
                    if (ReplayWorkers.IsEmpty)
                    {
                        Thread.Sleep(100);
                        continue;
                    }

                    foreach (ReplayWorker wrk in ReplayWorkers.Values)
                    {
                        if (wrk.LastCommandTime < DateTime.Now.AddSeconds(-InactiveWorkerTerminationTimeoutSeconds) && !wrk.HasCommands)
                        {
                            RemoveWorker(wrk.Name);
                        }
                    }

                    logger.Trace($"{ReplayWorkers.Count} registered active workers");
                    logger.Trace($"{ReplayWorkers.Min(x => x.Value.LastCommandTime)} oldest command date");
                }
                catch (Exception e)
                {
                    logger.Warn(e.Message);
                }

                Thread.Sleep(InactiveWorkerTerminationTimeoutSeconds * 1000); // sleep some seconds
            }
            logger.Trace("Sweeper thread stopped.");
        }



        private void RemoveWorker(string name)
        {
            ReplayWorker outWrk;
            ReplayWorkers.TryRemove(Int32.Parse(name), out outWrk);
            logger.Trace($"Worker [{name}] - Disposing");
            outWrk.Stop();
            outWrk.Dispose();
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
                    foreach (ReplayWorker wrk in ReplayWorkers.Values)
                    {
                        if (stopped) return;

                        if (wrk.IsStopped)
                        {
                            RemoveWorker(wrk.Name);
                        }

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
                            else if (SynchronizationMode == SynchronizationModeEnum.WorkerTask)
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
                                    logger.Error(e, "Exception in ReplayWorker.RunWorkers - WorkerTask");
                                }
                            }
                        }
                    };


                    if (SynchronizationMode == SynchronizationModeEnum.WorkerTask)
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
                catch (Exception ex)
                {
                    logger.Error(ex, "Exception in ReplayWorker.RunWorkers");
                }

            }


            if (SynchronizationMode == SynchronizationModeEnum.WorkerTask)
            {
                foreach (ReplayWorker wrk in ReplayWorkers.Values)
                {
                    wrk.Stop();
                    wrk.Dispose();
                }
            }
            logger.Trace("Worker thread stopped.");
        }

        public override bool HasMoreEvents()
        {
            return ReplayWorkers.Count(t => t.Value.HasCommands) > 0;
        }
    }
}
