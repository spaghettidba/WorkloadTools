using NLog;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.Model
{
    public class WorkloadAnalysis
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public ObservableCollection<WorkloadAnalysisPoint> Points { get; set; }

        public string Name { get; set; }

        public SqlConnectionInfo ConnectionInfo { get; set; }

        public void Load()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                Dictionary<long, NormalizedQuery> NormalizedQueries = new Dictionary<long, NormalizedQuery>();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM " + ConnectionInfo.SchemaName + ".NormalizedQueries";
                    using (var rdr = cmd.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            NormalizedQueries.Add(rdr.GetInt64(rdr.GetOrdinal("sql_hash")), new NormalizedQuery()
                            {
                                Hash = rdr.GetInt64(rdr.GetOrdinal("sql_hash")),
                                NormalizedText = rdr.GetString(rdr.GetOrdinal("normalized_text")),
                                ExampleText = rdr.GetString(rdr.GetOrdinal("example_text"))
                            });
                        }
                    }
                }

                using (SqlCommand cmd = conn.CreateCommand())
                {

                    string sqlText = WorkloadViewer.Properties.Resources.WorkloadAnalysis;
                    cmd.CommandText = sqlText.Replace("capture", ConnectionInfo.SchemaName);
                    using (var rdr = cmd.ExecuteReader())
                    {
                        Points = new ObservableCollection<WorkloadAnalysisPoint>();
                        while (rdr.Read())
                        {
                            try
                            {
                                WorkloadAnalysisPoint point = new WorkloadAnalysisPoint()
                                {
                                    OffsetMinutes = rdr.GetInt32(rdr.GetOrdinal("offset_minutes")),
                                    DurationMinutes = rdr.GetInt32(rdr.GetOrdinal("duration_minutes")),
                                    NormalizedQuery = NormalizedQueries[rdr.GetInt64(rdr.GetOrdinal("sql_hash"))],
                                    ApplicationName = rdr.GetString(rdr.GetOrdinal("application_name")),
                                    DatabaseName = rdr.GetString(rdr.GetOrdinal("database_name")),
                                    LoginName = rdr.GetString(rdr.GetOrdinal("login_name")),
                                    HostName = rdr.GetString(rdr.GetOrdinal("host_name")),
                                    AvgCpuMs = rdr.GetInt64(rdr.GetOrdinal("avg_cpu_ms")),
                                    MinCpuMs = rdr.GetInt64(rdr.GetOrdinal("min_cpu_ms")),
                                    MaxCpuMs = rdr.GetInt64(rdr.GetOrdinal("max_cpu_ms")),
                                    SumCpuMs = rdr.GetInt64(rdr.GetOrdinal("sum_cpu_ms")),
                                    AvgReads = rdr.GetInt64(rdr.GetOrdinal("avg_reads")),
                                    MinReads = rdr.GetInt64(rdr.GetOrdinal("min_reads")),
                                    MaxReads = rdr.GetInt64(rdr.GetOrdinal("max_reads")),
                                    SumReads = rdr.GetInt64(rdr.GetOrdinal("sum_reads")),
                                    AvgWrites = rdr.GetInt64(rdr.GetOrdinal("avg_writes")),
                                    MinWrites = rdr.GetInt64(rdr.GetOrdinal("min_writes")),
                                    MaxWrites = rdr.GetInt64(rdr.GetOrdinal("max_writes")),
                                    SumWrites = rdr.GetInt64(rdr.GetOrdinal("sum_writes")),
                                    AvgDurationMs = rdr.GetInt64(rdr.GetOrdinal("avg_duration_ms")),
                                    MinDurationMs = rdr.GetInt64(rdr.GetOrdinal("min_duration_ms")),
                                    MaxDurationMs = rdr.GetInt64(rdr.GetOrdinal("max_duration_ms")),
                                    SumDurationMs = rdr.GetInt64(rdr.GetOrdinal("sum_duration_ms")),
                                    ExecutionCount = rdr.GetInt64(rdr.GetOrdinal("execution_count"))
                                };
                                Points.Add(point);
                            }
                            catch(Exception e)
                            {
                                logger.Warn($"Skipping invalid datapoint at {rdr.GetInt32(rdr.GetOrdinal("offset_minutes"))} because of Exception: {e.StackTrace}");
                            }
                        }
                    }
                }
            }
        }
    }
}
