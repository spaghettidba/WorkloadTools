using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using FastMember;

using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

using NLog;

using WorkloadTools.Util;

namespace WorkloadTools.Consumer.Analysis
{
    public partial class WorkloadAnalyzer : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public SqlConnectionInfo ConnectionInfo 
        { 
            get => connectionInfo;
            set { connectionInfo = value; if (databaseWriter != null) databaseWriter.ConnectionInfo = value; }
        }
        public int Interval 
        {
            get => interval; 
            set { interval = value; if(databaseWriter != null) databaseWriter.Interval = value; } 
        }

        private Queue<WorkloadEvent> _internalQueue = new Queue<WorkloadEvent>();
        private readonly object _internalQueueLock = new object();
        private Thread Worker;
        private bool stopped = false;
        public int MaxInternalQueueSize { get; set; } = 10000;

        public int MaximumWriteRetries 
        {
            get => maximumWriteRetries;
            set { maximumWriteRetries = value; if (databaseWriter != null) databaseWriter.MaximumWriteRetries = value; }
        }
        public bool TruncateTo4000 
        { 
            get => truncateTo4000;
            set { truncateTo4000 = value; if (databaseWriter != null) databaseWriter.TruncateTo4000 = value; }
        }
        public bool TruncateTo1024 
        { 
            get => truncateTo1024; 
            set { truncateTo1024 = value; if (databaseWriter != null) databaseWriter.TruncateTo1024 = value; }
        }
        public bool WriteDetail 
        { 
            get => writeDetail;
            set { writeDetail = value; if (databaseWriter != null) databaseWriter.WriteDetail = value; }
        }
        public bool WriteSummary
        {
            get => writeSummary;
            set { writeSummary = value; if (databaseWriter != null) databaseWriter.WriteSummary = value; }
        }

        private DateTime lastDump = DateTime.MinValue;
        private DateTime lastEventTime = DateTime.MinValue;

        
        private AnalysisDatabaseWriter databaseWriter;
        private WorkloadData workloadData;
        private bool dictionariesPopulated = false;
        private int interval;
        private SqlConnectionInfo connectionInfo;
        private int maximumWriteRetries;
        private bool truncateTo4000;
        private bool truncateTo1024;
        private bool writeDetail = true;
        private bool writeSummary = true;

        public WorkloadAnalyzer()
		{

            workloadData = new WorkloadData()
            {
                Normalizer = new SqlTextNormalizer()
                {
                    TruncateTo1024 = TruncateTo1024,
                    TruncateTo4000 = TruncateTo4000
                }
            };

            databaseWriter = new SqlServerAnalysisDatabaseWriter()
            {
                ConnectionInfo = ConnectionInfo,
                MaximumWriteRetries = MaximumWriteRetries,
                TruncateTo1024 = TruncateTo1024,
                TruncateTo4000 = TruncateTo4000,
                WriteDetail = WriteDetail,
                WriteSummary = WriteSummary,
                Data = workloadData,
                Interval = Interval
            };

        }

        public bool HasEventsQueued
        {
            get
            {
                lock (_internalQueueLock)
                {
                    return _internalQueue.Count > 0;
                }
            }
        }

        private void CloseInterval()
        {
            // Write collected data to the destination database
            var duration = lastEventTime - lastDump;
            if (duration.TotalMinutes >= Interval)
            {
                // Avoid writing the same interval_id twice. This can happen when
                // Interval=0 (the default) and multiple events share the same
                // second-precision timestamp: after the first write sets
                // lastDump=lastEventTime, the condition above is 0>=0 (always true),
                // so the next loop iteration would attempt to INSERT to WorkloadDetails
                // for an interval_id that was already committed.
                var prospectiveIntervalId = databaseWriter.ComputeIntervalId(lastEventTime);
                if (prospectiveIntervalId == databaseWriter.LastWrittenIntervalId)
                {
                    lastDump = lastEventTime;
                    return;
                }

                try
                {
                    var numRetries = 0;
                    while (numRetries <= MaximumWriteRetries)
                    {
                        try
                        {
                            databaseWriter.WriteToServer(lastEventTime);
                            numRetries = MaximumWriteRetries + 1;
                        }
                        catch (Exception ex)
                        {
                            logger.Warn("Unable to write workload analysis.");
                            logger.Warn(ex.Message);

                            if (numRetries == MaximumWriteRetries)
                            {
                                throw;
                            }
                        }
                        numRetries++;
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        logger.Error(e, "Unable to write workload analysis info to the destination database.");
                        logger.Error(e.StackTrace);
                    }
                    catch
                    {
                        Console.WriteLine(string.Format("Unable to write to the database: {0}.", e.Message));
                    }
                }
                finally
                {
                    lastDump = lastEventTime;
                }
            }

        }

        private void ProcessQueue()
        {
            while (!stopped)
            {
                WorkloadEvent data = null;
                bool hasData = false;

                lock (_internalQueueLock)
                {
                    CloseInterval();

                    if (_internalQueue.Count > 0)
                    {
                        data = _internalQueue.Dequeue();
                        hasData = true;
                        // Notify Add() that a slot is now free
                        Monitor.PulseAll(_internalQueueLock);
                    }
                }

                if (hasData)
                {
                    // Populates dictionaries from the database on the first execution
                    // event, which is guaranteed to arrive before any other event type
                    // due to the way events are generated in the trace processing code.
                    if (!dictionariesPopulated && data is ExecutionWorkloadEvent)
                    {
                        databaseWriter.ConnectionInfo = this.ConnectionInfo;
                        databaseWriter.PopulateDictionariesFromDatabase(workloadData);
                        dictionariesPopulated = true;
                    }

                    workloadData.InternalAdd(data);
                }
                else
                {
                    // Sleep outside the lock so Add() is not blocked unnecessarily
                    Thread.Sleep(10);
                }
            }
        }

        public void Add(WorkloadEvent evt)
        {
            if (evt is ExecutionWorkloadEvent executionEvent && string.IsNullOrEmpty(executionEvent.Text))
            {
                return;
            }

            try
            {
                ProvisionWorker();
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to start the worker thread for WorkloadAnalyzer");
            }

            lock (_internalQueueLock)
            {
                // Block when the queue is full to avoid unbounded memory growth
                while (!stopped && _internalQueue.Count >= MaxInternalQueueSize)
                {
                    Monitor.Wait(_internalQueueLock);
                }

                if (stopped) return;

                lastEventTime = evt.StartTime;
                if (lastDump == DateTime.MinValue)
                {
                    lastDump = lastEventTime;
                }
                _internalQueue.Enqueue(evt);
            }

        }

        private void ProvisionWorker()
        {
            var startNewWorker = false;
            if (Worker == null)
            {
                startNewWorker = true;
            }
            else
            {
                if (!Worker.IsAlive)
                {
                    startNewWorker = true;
                }
            }

            if (startNewWorker)
            {
                // Start a new background worker if the thread is null
                // or stopped / aborted
                Worker = new Thread(() =>
                {
                    try
                    {
                        ProcessQueue();
                    }
                    catch (Exception e)
                    {
                        logger.Error(e.Message);
                        logger.Error(e.StackTrace);
                    }
                })
                {
                    IsBackground = true,
                    Name = "RealtimeWorkloadAnalyzer.Worker"
                };
                Worker.Start();

                Thread.Sleep(100);
            }
        }

        public void Stop()
        {
            try
            {
                databaseWriter.WriteToServer(lastEventTime);
            }
            catch (Exception e)
            {
                // duplicate key errors might be thrown at this time
                // that's expected if trying to upload to the same
                // interval already uploaded and new queries with the 
                // same hash have been captured
                if(!e.Message.Contains("Violation of PRIMARY KEY"))
                {
                    throw;
                }
            }
            stopped = true;
            // Wake up any Add() calls that may be blocked waiting for queue space
            lock (_internalQueueLock)
            {
                Monitor.PulseAll(_internalQueueLock);
            }
        }

        public void Dispose()
        {
            workloadData?.Dispose();
        }



    }
}

