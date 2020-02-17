using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WorkloadTools.Consumer.Analysis;

namespace WorkloadTools.Consumer.Analysis
{
    public class AnalysisConsumer : BufferedWorkloadConsumer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        private WorkloadAnalyzer analyzer;

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int UploadIntervalSeconds { get; set; }
		public int MaximumWriteRetries { get; set; } = 5;

		public bool SqlNormalizerTruncateTo4000 { get; set; }
		public bool SqlNormalizerTruncateTo1024 { get; set; }

		public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if(analyzer == null)
            {
                analyzer = new WorkloadAnalyzer()
                {
                    Interval = UploadIntervalSeconds / 60,
                    ConnectionInfo = this.ConnectionInfo,
					MaximumWriteRetries = this.MaximumWriteRetries,
					TruncateTo1024 = this.SqlNormalizerTruncateTo1024,
					TruncateTo4000 = this.SqlNormalizerTruncateTo4000
				};
            }

            analyzer.Add(evt);
        }

        public override bool HasMoreEvents()
        {
            return analyzer.HasEventsQueued;
        }

        protected override void Dispose(bool disposing)
        {
            if (analyzer != null)
            {
                analyzer.Stop();
                analyzer.Dispose();
            }
        }

    }
}
