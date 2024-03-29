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
        public bool RelativeDelays { get; set; } = false;

        private LogLevel _CommandErrorLogLevel = LogLevel.Error;
        public string CommandErrorLogLevel
        {
            get => _CommandErrorLogLevel.Name;
            set => _CommandErrorLogLevel = LogLevel.FromString(value);
        }

        public Dictionary<string, string> DatabaseMap { get; set; } = new Dictionary<string, string>();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public ThreadingModeEnum ThreadingMode { get; set; } = ThreadingModeEnum.WorkerTask;

        private readonly ConcurrentDictionary<string, ReplayWorker> ReplayWorkers = new ConcurrentDictionary<string, ReplayWorker>();

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

        private string WorkerKey(ExecutionWorkloadEvent evnt)
        {
            // In SQL the SPID is only unqiue while the session is in use.
            // When the SPID is reused it may be for a different database.

            var result = $"{evnt.SPID}_{evnt.DatabaseName}";

            if (MimicApplicationName)
            {
                // When the SPID is reused it may be for a different Host, User or Application
                // but this application can only mimic the Application Name so if we're doing
                // that include that in the key.
                result += $"_{evnt.ApplicationName}";
            }

            return result;
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
                    var percentInfo = (double)eventCount / (double)totalEventCount;
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

            if (stopped) { return; }

            var command = new ReplayCommand()
            {
                CommandText = evt.Text,
                Database = evt.DatabaseName,
                ApplicationName = evt.ApplicationName,
                ReplayOffset = evt.ReplayOffset,
                StartTime = evt.StartTime,
                EventSequence = evt.EventSequence
            };

            var workerKey = WorkerKey(evt);

            if (ReplayWorkers.TryGetValue(workerKey, out var rw))
            {
                // Ensure that the buffer does not get too big
                while (rw.QueueLength >= (BufferSize * .9))
                {
                    spin.SpinOnce();
                }

                if (stopped) { return; }

                rw.AppendCommand(command);
            }
            else
            {
                logger.Debug("Creating Worker {Worker}", workerKey);

                rw = new ReplayWorker(workerKey)
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
                    RaiseErrorsToSqlEventTracing = RaiseErrorsToSqlEventTracing,
                    RelativeDelays = RelativeDelays
                };

                rw.AppendCommand(command);

                if (stopped) { return; }
                _ = ReplayWorkers.TryAdd(workerKey, rw);
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
            logger.Info("Disposing ReplayConsumer");
            stopped = true;

            foreach (var r in ReplayWorkers.Values)
            {
                r.Dispose();
            }
            WorkLimiter.Dispose();
        }

        // Sweeper thread: removes from the workers list all the workers
        // that have not executed a command in the last 5 minutes
        private void RunSweeper()
        {
            while (!stopped)
            {
                logger.Debug("Looking for workers that have been idle for {InactiveWorkerTerminationTimeoutSeconds}s", InactiveWorkerTerminationTimeoutSeconds);

                try
                {
                    // Use .ToList() to materialise the list so that ReplayWorkers.TryRemove does not cause an exception that the list has changed during the iteration
                    foreach (var wrk in ReplayWorkers.Values.Where(x => x.LastCommandTime > DateTime.MinValue && x.LastCommandTime < DateTime.Now.AddSeconds(-InactiveWorkerTerminationTimeoutSeconds) && !x.HasCommands).ToList())
                    {
                        if(stopped) { return; }

                        logger.Debug("Removing worker {Worker} which has not executed a command since {lastCommand}", wrk.Name, wrk.LastCommandTime);

                        RemoveWorker(wrk.Name);
                    }
                }
                catch (Exception e)
                {
                    logger.Warn(e, "Error when removing idle workers");
                }

                Thread.Sleep(InactiveWorkerTerminationTimeoutSeconds * 1000); // sleep some seconds
            }
        }

        private void RemoveWorker(string name)
        {
            _ = ReplayWorkers.TryRemove(name, out var outWrk);

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
                        if (!wrk.IsRunning && !stopped)
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
