using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using NLog;

namespace WorkloadTools.Consumer.Analysis
{
    /// <summary>
    /// Provides generalized logic for aggregating workload data.
    /// This class is database-agnostic and can be used by any database writer implementation.
    /// </summary>
    public class WorkloadSummaryAggregator
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// Represents aggregated execution summary data
        /// </summary>
        public class ExecutionSummaryRecord
        {
            public int ApplicationId { get; set; }
            public int DatabaseId { get; set; }
            public int HostId { get; set; }
            public int LoginId { get; set; }
            public long MinCpuUs { get; set; }
            public long MaxCpuUs { get; set; }
            public long SumCpuUs { get; set; }
            public long MinReads { get; set; }
            public long MaxReads { get; set; }
            public long SumReads { get; set; }
            public long MinWrites { get; set; }
            public long MaxWrites { get; set; }
            public long SumWrites { get; set; }
            public long MinDurationUs { get; set; }
            public long MaxDurationUs { get; set; }
            public long SumDurationUs { get; set; }
            public DateTime MinExecutionDate { get; set; }
            public DateTime MaxExecutionDate { get; set; }
            public long ExecutionCount { get; set; }
        }

        /// <summary>
        /// Represents aggregated execution details data
        /// </summary>
        public class ExecutionDetailsRecord
        {
            public int IntervalId { get; set; }
            public long SqlHash { get; set; }
            public int ApplicationId { get; set; }
            public int DatabaseId { get; set; }
            public int HostId { get; set; }
            public int LoginId { get; set; }
            public double AvgCpuUs { get; set; }
            public long MinCpuUs { get; set; }
            public long MaxCpuUs { get; set; }
            public long SumCpuUs { get; set; }
            public double AvgReads { get; set; }
            public long MinReads { get; set; }
            public long MaxReads { get; set; }
            public long SumReads { get; set; }
            public double AvgWrites { get; set; }
            public long MinWrites { get; set; }
            public long MaxWrites { get; set; }
            public long SumWrites { get; set; }
            public double AvgDurationUs { get; set; }
            public long MinDurationUs { get; set; }
            public long MaxDurationUs { get; set; }
            public long SumDurationUs { get; set; }
            public long ExecutionCount { get; set; }
        }

        /// <summary>
        /// Aggregates execution summary data from raw workload data
        /// </summary>
        public IEnumerable<ExecutionSummaryRecord> AggregateExecutionSummary(WorkloadData workloadData)
        {
            if (workloadData == null)
                throw new ArgumentNullException(nameof(workloadData));

            lock (workloadData)
            {
                var summaryRecords = from t in workloadData.RawData.Keys
                                     from v in workloadData.RawData[t]
                                     group new
                                     {
                                         v.Cpu_us,
                                         v.Duration_us,
                                         v.Event_time,
                                         v.Reads,
                                         v.Writes
                                     }
                                     by new
                                     {
                                         application_id = t.Application_id,
                                         database_id = t.Database_id,
                                         host_id = t.Host_id,
                                         login_id = t.Login_id
                                     }
                                     into grp
                                     select new ExecutionSummaryRecord
                                     {
                                         ApplicationId = grp.Key.application_id,
                                         DatabaseId = grp.Key.database_id,
                                         HostId = grp.Key.host_id,
                                         LoginId = grp.Key.login_id,
                                         MinCpuUs = (long)(grp.Min(v => v.Cpu_us) ?? 0),
                                         MaxCpuUs = (long)(grp.Max(v => v.Cpu_us) ?? 0),
                                         SumCpuUs = (long)(grp.Sum(v => v.Cpu_us) ?? 0),
                                         MinReads = (long)(grp.Min(v => v.Reads) ?? 0),
                                         MaxReads = (long)(grp.Max(v => v.Reads) ?? 0),
                                         SumReads = (long)(grp.Sum(v => v.Reads) ?? 0),
                                         MinWrites = (long)(grp.Min(v => v.Writes) ?? 0),
                                         MaxWrites = (long)(grp.Max(v => v.Writes) ?? 0),
                                         SumWrites = (long)(grp.Sum(v => v.Writes) ?? 0),
                                         MinDurationUs = (long)(grp.Min(v => v.Duration_us) ?? 0),
                                         MaxDurationUs = (long)(grp.Max(v => v.Duration_us) ?? 0),
                                         SumDurationUs = (long)(grp.Sum(v => v.Duration_us) ?? 0),
                                         MinExecutionDate = grp.Min(v => v.Event_time),
                                         MaxExecutionDate = grp.Max(v => v.Event_time),
                                         ExecutionCount = grp.Count()
                                     };

                return summaryRecords.ToList();
            }
        }

        /// <summary>
        /// Aggregates execution details data from raw workload data
        /// </summary>
        public IEnumerable<ExecutionDetailsRecord> AggregateExecutionDetails(WorkloadData workloadData, int intervalId)
        {
            if (workloadData == null)
                throw new ArgumentNullException(nameof(workloadData));

            lock (workloadData)
            {
                var detailRecords = from t in workloadData.RawData.Keys
                                    from v in workloadData.RawData[t]
                                    group new
                                    {
                                        v.Cpu_us,
                                        v.Duration_us,
                                        v.Event_time,
                                        v.Reads,
                                        v.Writes
                                    }
                                    by new
                                    {
                                        sql_hash = t.Sql_hash,
                                        application_id = t.Application_id,
                                        database_id = t.Database_id,
                                        host_id = t.Host_id,
                                        login_id = t.Login_id
                                    }
                                    into grp
                                    select new ExecutionDetailsRecord
                                    {
                                        IntervalId = intervalId,
                                        SqlHash = grp.Key.sql_hash,
                                        ApplicationId = grp.Key.application_id,
                                        DatabaseId = grp.Key.database_id,
                                        HostId = grp.Key.host_id,
                                        LoginId = grp.Key.login_id,
                                        AvgCpuUs = grp.Average(v => v.Cpu_us) ?? 0,
                                        MinCpuUs = (long)(grp.Min(v => v.Cpu_us) ?? 0),
                                        MaxCpuUs = (long)(grp.Max(v => v.Cpu_us) ?? 0),
                                        SumCpuUs = (long)(grp.Sum(v => v.Cpu_us) ?? 0),
                                        AvgReads = grp.Average(v => v.Reads) ?? 0,
                                        MinReads = (long)(grp.Min(v => v.Reads) ?? 0),
                                        MaxReads = (long)(grp.Max(v => v.Reads) ?? 0),
                                        SumReads = (long)(grp.Sum(v => v.Reads) ?? 0),
                                        AvgWrites = grp.Average(v => v.Writes) ?? 0,
                                        MinWrites = (long)(grp.Min(v => v.Writes) ?? 0),
                                        MaxWrites = (long)(grp.Max(v => v.Writes) ?? 0),
                                        SumWrites = (long)(grp.Sum(v => v.Writes) ?? 0),
                                        AvgDurationUs = grp.Average(v => v.Duration_us) ?? 0,
                                        MinDurationUs = (long)(grp.Min(v => v.Duration_us) ?? 0),
                                        MaxDurationUs = (long)(grp.Max(v => v.Duration_us) ?? 0),
                                        SumDurationUs = (long)(grp.Sum(v => v.Duration_us) ?? 0),
                                        ExecutionCount = grp.Count()
                                    };

                return detailRecords.ToList();
            }
        }

        /// <summary>
        /// Aggregates wait stats data by wait type
        /// </summary>
        public IEnumerable<dynamic> AggregateWaitStats(DataTable waitsData, int intervalId)
        {
            if (waitsData == null)
                return Enumerable.Empty<dynamic>();

            lock (waitsData)
            {
                var waitRecords = from t in waitsData.AsEnumerable()
                                  group t by new
                                  {
                                      wait_type = t.Field<string>("wait_type")
                                  }
                                  into grp
                                  select new
                                  {
                                      interval_id = intervalId,
                                      grp.Key.wait_type,
                                      wait_sec = grp.Sum(t => t.Field<double>("wait_sec")),
                                      resource_sec = grp.Sum(t => t.Field<double>("resource_sec")),
                                      signal_sec = grp.Sum(t => t.Field<double>("signal_sec")),
                                      wait_count = grp.Sum(t => t.Field<double>("wait_count"))
                                  };

                return waitRecords.ToList();
            }
        }

        /// <summary>
        /// Aggregates disk performance data by file identifiers
        /// </summary>
        public IEnumerable<dynamic> AggregateDiskPerf(DataTable diskPerfData, int intervalId)
        {
            if (diskPerfData == null)
                return Enumerable.Empty<dynamic>();

            lock (diskPerfData)
            {
                var diskRecords = from t in diskPerfData.AsEnumerable()
                                  group t by new
                                  {
                                      database_name = t.Field<string>("database_name"),
                                      physical_filename = t.Field<string>("physical_filename"),
                                      logical_filename = t.Field<string>("logical_filename"),
                                      file_type = t.Field<string>("file_type"),
                                      volume_mount_point = t.Field<string>("volume_mount_point"),
                                  }
                                  into grp
                                  select new
                                  {
                                      interval_id = intervalId,
                                      grp.Key.database_name,
                                      grp.Key.physical_filename,
                                      grp.Key.logical_filename,
                                      grp.Key.file_type,
                                      grp.Key.volume_mount_point,
                                      read_latency_ms = grp.Average(t => t.Field<double>("read_latency_ms")),
                                      reads = grp.Sum(t => t.Field<double>("reads")),
                                      read_bytes = grp.Sum(t => t.Field<double>("read_bytes")),
                                      write_latency_ms = grp.Average(t => t.Field<double>("write_latency_ms")),
                                      writes = grp.Sum(t => t.Field<double>("writes")),
                                      write_bytes = grp.Sum(t => t.Field<double>("write_bytes")),
                                      cum_read_latency_ms = grp.Max(t => t.Field<double?>("cum_read_latency_ms")),
                                      cum_reads = grp.Max(t => t.Field<double?>("cum_reads")),
                                      cum_read_bytes = grp.Max(t => t.Field<double?>("cum_read_bytes")),
                                      cum_write_latency_ms = grp.Max(t => t.Field<double?>("cum_write_latency_ms")),
                                      cum_writes = grp.Max(t => t.Field<double?>("cum_writes")),
                                      cum_write_bytes = grp.Max(t => t.Field<double?>("cum_write_bytes"))
                                  };

                return diskRecords.ToList();
            }
        }

        /// <summary>
        /// Aggregates performance counters by counter name
        /// </summary>
        public IEnumerable<dynamic> AggregatePerformanceCounters(DataTable counterData, int intervalId)
        {
            if (counterData == null)
                return Enumerable.Empty<dynamic>();

            lock (counterData)
            {
                var counterRecords = from t in counterData.AsEnumerable()
                                     group t by new
                                     {
                                         counter_name = t.Field<string>("counter_name")
                                     }
                                     into grp
                                     select new
                                     {
                                         interval_id = intervalId,
                                         grp.Key.counter_name,
                                         min_counter_value = grp.Min(t => t.Field<float>("counter_value")),
                                         max_counter_value = grp.Max(t => t.Field<float>("counter_value")),
                                         avg_counter_value = grp.Average(t => t.Field<float>("counter_value"))
                                     };

                return counterRecords.ToList();
            }
        }

        /// <summary>
        /// Aggregates error data by error type and message
        /// </summary>
        public IEnumerable<dynamic> AggregateErrors(DataTable errorData, int intervalId)
        {
            if (errorData == null)
                return Enumerable.Empty<dynamic>();

            lock (errorData)
            {
                var errorRecords = from t in errorData.AsEnumerable()
                                   group t by new
                                   {
                                       type = t.Field<int>("type"),
                                       message = t.Field<string>("message")
                                   }
                                   into grp
                                   select new
                                   {
                                       interval_id = intervalId,
                                       error_type = ((WorkloadEvent.EventType)grp.Key.type).ToString(),
                                       grp.Key.message,
                                       error_count = grp.Count()
                                   };

                return errorRecords.ToList();
            }
        }
    }
}
