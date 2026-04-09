using System;
using System.CodeDom.Compiler;
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
        private long dispatchedEventCount;
        private DateTime startTime = DateTime.MinValue;

        // holds the total number of events to replay
        // only available when reading from a file
        // for realtime replays this is not available
        private long totalEventCount = 0;

        // holds the number of events that have been executed by the workers
        private long executedEventCount;

        // watchdog: fires if no command has been executed for WatchdogIntervalSeconds
        public int WatchdogIntervalSeconds { get; set; } = 30;
        private Timer watchdogTimer;

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

        private void EnsureWatchdogRunning()
        {
            if (watchdogTimer != null)
            {
                return;
            }

            var intervalMs = WatchdogIntervalSeconds * 1000;
            watchdogTimer = new Timer(
                delegate
                {
                    var current = Interlocked.Read(ref executedEventCount);
                    var dispatched = Interlocked.Read(ref dispatchedEventCount);
                    var bufferedEventCount = ReplayWorkers.Values.Sum(x => x.QueueLength);

                    // Always log on every watchdog tick so the user can see progress
                    // (or lack thereof) at wall-clock intervals, regardless of whether
                    // the event count has crossed a modulus boundary.
                    LogReplayProgress(current, forceLog: true);

                    // Check for completion: all dispatched events have been executed
                    // and nothing is left in any buffer.
                    if (dispatched > 0
                        && current >= dispatched
                        && Buffer.Count == 0
                        && bufferedEventCount == 0)
                    {
                        // Stop the watchdog - nothing left to watch.
                        watchdogTimer?.Dispose();
                        watchdogTimer = null;
                    }
                },
                null,
                intervalMs,
                intervalMs);
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

            // dispatchedEventCount tracks only the ExecutionWorkloadEvents that have been
            // dispatched to a worker, giving a stable monotonic counter to drive log intervals.
            dispatchedEventCount++;

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

                rw.CommandExecuted += OnWorkerCommandExecuted;

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

            EnsureWatchdogRunning();
        }

        protected override void Dispose(bool disposing)
        {
            logger.Info("Disposing ReplayConsumer");
            stopped = true;

            watchdogTimer?.Dispose();
            watchdogTimer = null;

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
                outWrk.CommandExecuted -= OnWorkerCommandExecuted;
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
                        // Using a semaphore to avoid overwhelming the threadpool
                        // Without this precaution, the memory consumption goes to the roof
                        _ = WorkLimiter.WaitOne();

                        var queued = false;
                        try
                        {
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
                                    finally
                                    {
                                        // Release only after execution completes to actually
                                        // bound the number of concurrently executing commands.
                                        _ = WorkLimiter.Release();
                                    }
                                }
                                );
                            queued = true;
                        }
                        finally
                        {
                            // If queuing itself failed, release the slot we acquired
                            // so the semaphore is not permanently leaked.
                            if (!queued) { _ = WorkLimiter.Release(); }
                        }
                    }
                    else if (ThreadingMode == ThreadingModeEnum.Tasks)
                    {
                        // TODO: Is this not the same as WorkerTask?
                        // Here the task is created by ReplayConsumer.
                        // With WorkerTask the task is created by ReplayWorker.Start.

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
                                finally
                                {
                                    // Release only after execution completes to actually
                                    // bound the number of concurrently executing commands.
                                    _ = WorkLimiter.Release();
                                }
                            }
                            );
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
            return ReplayWorkers.Count(t => t.Value.HasCommands) > 0 || Buffer.Count > 0;
        }

        private long completionLogged = 0;

        private void OnWorkerCommandExecuted(object sender, EventArgs e)
        {
            var executed = Interlocked.Increment(ref executedEventCount);
            EnsureWatchdogRunning();
            LogReplayProgress(executed);

            // Log completion eagerly as soon as the last command is executed,
            // without waiting for the next watchdog tick which may never fire
            // if the controller disposes first.
            // Only applies when totalEventCount is known (i.e. file-based replay).
            // For realtime replay, totalEventCount is 0 and there is no defined end.
            if (totalEventCount > 0)
            {
                var dispatched = Interlocked.Read(ref dispatchedEventCount);
                if (executed >= dispatched
                    && Buffer.Count == 0
                    && ReplayWorkers.Values.Sum(x => x.QueueLength) == 0
                    && Interlocked.CompareExchange(ref completionLogged, 1, 0) == 0)
                {
                    LogReplayProgress(executed, forceLog: true);
                    logger.Info("Replay completed: {executed} commands executed out of {dispatched} dispatched", executed, dispatched);
                }
            }
        }

        private void LogReplayProgress(long executed, bool forceLog = false)
        {
            // Determine the log interval:
            // - If dispatchedEventCount is known and > 0: aim for ~1000 log lines for large workloads,
            //   fall back to ~10 for small ones (< 1000 events).
            // - If dispatchedEventCount is unknown: fall back to WorkerStatsCommandCount.
            var dispatched = Interlocked.Read(ref dispatchedEventCount);

            long logInterval;
            if (dispatched > 0)
            {
                var fineInterval = dispatched / 1000;
                var coarseInterval = Math.Max(1, dispatched / 10);
                logInterval = fineInterval >= 1 ? fineInterval : coarseInterval;
            }
            else
            {
                logInterval = Math.Max(1, WorkerStatsCommandCount);
            }

            if (forceLog || executed == 1 || executed % logInterval == 0)
            {
                var bufferedEventCount = ReplayWorkers.Values.Sum(x => x.QueueLength);
                if (dispatched > 0)
                {
                    var percentInfo = (double)executed / (double)dispatched;
                    logger.Info($"{executed} ({percentInfo:P}) events replayed - {Buffer.Count + bufferedEventCount} events buffered");
                }
                else
                {
                    logger.Info($"{executed} events replayed - {Buffer.Count + bufferedEventCount} events buffered");
                }
            }
        }
    }
}
