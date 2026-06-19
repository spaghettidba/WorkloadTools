using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using FastMember;

using NLog;

using WorkloadTools.Util;

using NFX.DataAccess;
using System.Data.Common;

namespace WorkloadTools.Consumer.Analysis
{
    public abstract class AnalysisDatabaseWriter
    {
        protected static readonly Logger logger = LogManager.GetCurrentClassLogger();
        protected readonly WorkloadSummaryAggregator _aggregator = new WorkloadSummaryAggregator();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int Interval { get; set; }
        public int MaximumWriteRetries { get; set; }
        public bool TruncateTo4000 { get; set; }
        public bool TruncateTo1024 { get; set; }
        public bool WriteDetail { get; set; } = true;
        public bool WriteSummary { get; set; } = true;

        protected bool TargetTableCreated = false;
        protected bool FirstIntervalWritten = false;
        public volatile int LastWrittenIntervalId = -1;

        public WorkloadData Data { get; set; }

        protected AnalysisDatabaseWriter() { }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public abstract void WriteToServer(DateTime intervalTime);

        protected abstract void WriteToTable(DataTable dt, string tableName);
        protected abstract void WriteWaitsData(int current_interval_id);
        protected abstract void WriteDiskPerf(int current_interval_id);
        protected abstract void WritePerformanceCounters(int current_interval_id);
        protected abstract void WriteExecutionSummary();
        protected abstract void WriteExecutionDetails(int current_interval_id);
        protected abstract void WriteExecutionErrors(int current_interval_id);
        protected abstract void WriteDictionary(Dictionary<string, int> values, string name);
        protected abstract void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values);
        protected abstract int CreateInterval(DateTime intervalTime);
        protected abstract void CreateTargetTables();
		protected abstract void CreateTargetDatabase();
		protected abstract void PopulateDictionariesFromDatabaseInternal(WorkloadData data);
		protected abstract void AddAllRowsInternal(DbConnection conn, string sql, Dictionary<string, int> d);

        protected void InvokeGC()
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
		}

		public int ComputeIntervalId(DateTime intervalTime)
		{
			// interval id is the number of seconds since 01/01/2000
			return (int)intervalTime.Subtract(DateTime.MinValue.AddYears(1999)).TotalSeconds;
		}

		/// <summary>
		/// Populates dictionaries in WorkloadData (Applications, Databases, Hosts, Logins)
		/// reading from the analysis database
		/// </summary>
		public void PopulateDictionariesFromDatabase(WorkloadData data)
		{
			if (data == null) throw new ArgumentNullException(nameof(data));

			CreateTargetDatabase();
			PopulateDictionariesFromDatabaseInternal(data);
		}
    }
}
