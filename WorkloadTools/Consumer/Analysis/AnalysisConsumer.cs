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
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();
        private WorkloadAnalyzer analyzer;

        private int _uploadIntervalSeconds;

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int UploadIntervalSeconds {
            get
            {
                return _uploadIntervalSeconds;
            }
            set
            {
                if(value % 60 != 0)
                {
                    throw new ArgumentOutOfRangeException("UploadIntervalSeconds must be an exact multiple of 60");
                }
                _uploadIntervalSeconds = value;
            }
        }

        public int UploadIntervalMinutes
        {
            get { return _uploadIntervalSeconds / 60; }
            set { _uploadIntervalSeconds = value * 60; }
        }

		public int MaximumWriteRetries { get; set; } = 5;

		public bool SqlNormalizerTruncateTo4000 { get; set; }
		public bool SqlNormalizerTruncateTo1024 { get; set; }

        public bool WriteDetail { get; set; } = true;
        public bool WriteSummary { get; set; } = true;

        public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if(analyzer == null)
            {
                analyzer = new WorkloadAnalyzer()
                {
                    Interval = UploadIntervalSeconds / 60,
                    ConnectionInfo = ConnectionInfo,
					MaximumWriteRetries = MaximumWriteRetries,
					TruncateTo1024 = SqlNormalizerTruncateTo1024,
					TruncateTo4000 = SqlNormalizerTruncateTo4000,
                    WriteDetail = WriteDetail
				};
            }

            analyzer.Add(evt);
        }

        public override bool HasMoreEvents()
        {
            return analyzer.HasEventsQueued || !Buffer.IsEmpty;
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
