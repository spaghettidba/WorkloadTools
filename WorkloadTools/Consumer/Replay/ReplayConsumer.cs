using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NLog;

namespace WorkloadTools.Consumer.Replay
{
    public class ReplayConsumer : BufferedWorkloadConsumer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private SpinWait spin = new SpinWait();
        public int ThreadLimit = 256;//32
        public int InactiveWorkerTerminationTimeoutSeconds = 300;
        private readonly Semaphore WorkLimiter;

        public bool DisplayWorkerStats { get; set; } = true;
        public bool ConsumeResults { get; set; } = true;
        public int QueryTimeoutSeconds { get; set; } = 30;
        public int WorkerStatsCommandCount { get; set; } = 1000;
        public bool MimicApplicationName { get; set; } = false;
        public int FailRetryCount { get; set; } = 0;
        public int TimeoutRetryCount { get; set; } = 0;
        public bool RaiseErrorsToSqlEventTracing { get; private set; } = true;

        private LogLevel _CommandErrorLogLevel = LogLevel.Error;
        public string CommandErrorLogLevel
        {
            get => _CommandErrorLogLevel.Name;
            set => _CommandErrorLogLevel = LogLevel.FromString(value);
        }

        public Dictionary<string, string> DatabaseMap { get; set; } = new Dictionary<string, string>();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public ThreadingModeEnum ThreadingMode { get; set; } = ThreadingModeEnum.WorkerTask;

        private readonly ConcurrentDictionary<int, ReplayWorker> ReplayWorkers = new ConcurrentDictionary<int, ReplayWorker>();

        private Thread sweeper;

        private long eventCount;
        private DateTime startTime = DateTime.MinValue;

        // holds the total number of events to replay
        // only available when reading from a file
        // for realtime replays this is not available
        private long totalEventCount = 0;

        public enum ThreadingModeEnum : int
        {
            ThreadPools = 1,
            Tasks = 2,
            WorkerTask = 3,
            Serial = 4
        }

        public ReplayConsumer()
        {
            WorkLimiter = new Semaphore(ThreadLimit, ThreadLimit);
        }

        public override void ConsumeBuffered(WorkloadEvent evnt)
        {
            if (evnt is MessageWorkloadEvent messageEvent)
            {
                if (messageEvent.MsgType == MessageWorkloadEvent.MessageType.TotalEvents)
                {
                    try
                    {
                        totalEventCount = (long)messageEvent.Value;
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, $"Unable to set the total number of events");
                    }
                }
            }

            // totalEventCount is EVERY Event except the initial MessageWorkloadEvent for the TotalEvents,
            // so always increment the counter.
            eventCount++;

            if (!(evnt is ExecutionWorkloadEvent))
            {
                return;
            }

            if (evnt.Type != WorkloadEvent.EventType.RPCStarting && evnt.Type != WorkloadEvent.EventType.BatchStarting)
            {
                return;
            }

            if (totalEventCount > 0)
            {
                if (eventCount % (totalEventCount / 1000) == 0)
                {
                    var percentInfo = eventCount / totalEventCount;
                    logger.Info("{eventCount} ({percentInfo:P}) events replayed - {bufferedEventCount} events buffered", eventCount, percentInfo, Buffer.Count);
                }
            }
            else
            {
                if (eventCount % WorkerStatsCommandCount == 0)
                {
                    logger.Info("{eventCount} events replayed - {bufferedEventCount} events buffered", eventCount, Buffer.Count);
                }
            }

            if (startTime == DateTime.MinValue)
            {
                // Pad the start time so that the first event isn't behind by the time the worker has started up on a thread.
                startTime = DateTime.Now.AddTicks(TimeSpan.TicksPerSecond);
                logger.Info("All future delays will be calculated from this point + 1s, triggered by event {@event}", evnt);
            }

            var evt = (ExecutionWorkloadEvent)evnt;

            var command = new ReplayCommand()
            {
                CommandText = evt.Text,
                Database = evt.DatabaseName,
                ApplicationName = evt.ApplicationName,
                ReplayOffset = evt.ReplayOffset,
                StartTime = evt.StartTime,
                EventSequence = evt.EventSequence
            };

            var session_id = -1;
            session_id = (int)evt.SPID;

            if (ReplayWorkers.TryGetValue(session_id, out var rw))
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
                logger.Debug("Creating Worker {Worker}", session_id);

                rw = new ReplayWorker(session_id.ToString())
                {
                    ConnectionInfo = ConnectionInfo,
                    ReplayIntervalSeconds = 0,
                    StopOnError = false,
                    DisplayWorkerStats = DisplayWorkerStats,
                    ConsumeResults = ConsumeResults,
                    QueryTimeoutSeconds = QueryTimeoutSeconds,
                    WorkerStatsCommandCount = WorkerStatsCommandCount,
                    MimicApplicationName = MimicApplicationName,
                    DatabaseMap = DatabaseMap,
                    StartTime = startTime,
                    FailRetryCount = FailRetryCount,
                    TimeoutRetryCount = TimeoutRetryCount,
                    CommandErrorLogLevel = _CommandErrorLogLevel,
                    RaiseErrorsToSqlEventTracing = RaiseErrorsToSqlEventTracing
                };

                _ = ReplayWorkers.TryAdd(session_id, rw);
                rw.AppendCommand(command);
            }

            // Ensure the worker is running.
            // If new it needs starting for the first time.
            // If existing it may have stopped if the command queue became empty.
            RunWorker(rw);

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
                                ))
                {
                    IsBackground = true
                };
                sweeper.Start();
            }
        }

        protected override void Dispose(bool disposing)
        {
            foreach (var r in ReplayWorkers.Values)
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

                    foreach (var wrk in ReplayWorkers.Values)
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
            logger.Trace("Sweeper thread stopped");
        }

        private void RemoveWorker(string name)
        {
            _ = ReplayWorkers.TryRemove(int.Parse(name), out var outWrk);

            logger.Trace("Disposing worker [{Worker}]", name);
            if (outWrk != null)
            {
                outWrk.Stop();
                outWrk.Dispose();
            }
        }

        private void RunWorker(ReplayWorker wrk)
        {
            try
            {
                if (stopped)
                {
                    return;
                }

                if (wrk.HasCommands)
                {
                    if (ThreadingMode == ThreadingModeEnum.ThreadPools)
                    {
                        try
                        {
                            // Using a semaphore to avoid overwhelming the threadpool
                            // Without this precaution, the memory consumption goes to the roof
                            _ = WorkLimiter.WaitOne();

                            // Queue the execution of a statement in the threadpool.
                            // The statement will get executed in a separate thread eventually.
                            _ = ThreadPool.QueueUserWorkItem(
                                delegate
                                {
                                    try
                                    {
                                        wrk.ExecuteNextCommand();
                                    }
                                    catch (Exception e)
                                    {
                                        try
                                        {
                                            logger.Error(e, "Unhandled exception in ReplayWorker.ExecuteCommand");
                                        }
                                        catch
                                        {
                                            Console.WriteLine(e.Message);
                                        }
                                    }
                                }
                                );
                        }
                        finally
                        {
                            // Release the semaphore
                            _ = WorkLimiter.Release();
                        }
                    }
                    else if (ThreadingMode == ThreadingModeEnum.Tasks)
                    {
                        // TODO: Is this not the same as WorkerTask?
                        // Here the task is created by ReplayConsumer.
                        // With WorkerTask the task is created by ReplayWorker.Start.
                        try
                        {
                            // Using a semaphore to avoid overwhelming the threadpool
                            // Without this precaution, the memory consumption goes to the roof
                            _ = WorkLimiter.WaitOne();

                            // Start a new Task to run the statement
                            var t = Task.Factory.StartNew(
                                delegate
                                {
                                    try
                                    {
                                        wrk.ExecuteNextCommand();
                                    }
                                    catch (Exception e)
                                    {
                                        try
                                        {
                                            logger.Error(e, "Unhandled exception in ReplayWorker.ExecuteCommand");
                                        }
                                        catch
                                        {
                                            Console.WriteLine(e.Message);
                                        }
                                    }
                                }
                                );
                        }
                        finally
                        {
                            // Release the semaphore
                            _ = WorkLimiter.Release();
                        }
                    }
                    else if (ThreadingMode == ThreadingModeEnum.WorkerTask)
                    {
                        if (!wrk.IsRunning)
                        {
                            wrk.Start();
                        }
                    }
                    else if (ThreadingMode == ThreadingModeEnum.Serial)
                    {
                        wrk.ExecuteNextCommand();
                    }
                }
            }
            catch (InvalidOperationException)
            {
                //ignore ...
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error starting worker");
            }
        }

        public override bool HasMoreEvents()
        {
            return ReplayWorkers.Count(t => t.Value.HasCommands) > 0;
        }
    }
}
