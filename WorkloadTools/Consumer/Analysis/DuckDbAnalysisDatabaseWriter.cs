using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using NLog;
using System.Data.Common;

namespace WorkloadTools.Consumer.Analysis
{
    internal class DuckDbAnalysisDatabaseWriter : AnalysisDatabaseWriter
    {
        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Warn("DuckDB database writing is not yet implemented");
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteWaitsData(int current_interval_id)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteDiskPerf(int current_interval_id)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WritePerformanceCounters(int current_interval_id)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteExecutionSummary()
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteExecutionDetails(int current_interval_id)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteExecutionErrors(int current_interval_id)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteDictionary(Dictionary<string, int> values, string name)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override int CreateInterval(DateTime intervalTime)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void CreateTargetTables()
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void CreateTargetDatabase()
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }

        protected override void AddAllRowsInternal(DbConnection conn, string sql, Dictionary<string, int> d)
        {
            throw new NotImplementedException("DuckDB support for analysis database writing is not yet implemented");
        }
    }
}
