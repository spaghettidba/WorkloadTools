using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WorkloadTools.Consumer.Analysis;

namespace WorkloadTools.Consumer
{
    public class AnalysisConsumer : BufferedWorkloadConsumer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        private WorkloadAnalyzer analyzer;

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int UploadIntervalSeconds { get; set; }

        public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if(analyzer == null)
            {
                analyzer = new WorkloadAnalyzer()
                {
                    Interval = UploadIntervalSeconds
                };
            }

            analyzer.Add(evt);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose();
            if (analyzer != null)
                analyzer.Stop();
        }

    }
}
