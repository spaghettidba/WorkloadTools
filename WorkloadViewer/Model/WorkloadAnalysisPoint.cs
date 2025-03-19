using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.Model
{
    public class WorkloadAnalysisPoint
    {
        public int OffsetMinutes { get; set; }
        public int DurationMinutes { get; set; }
        public NormalizedQuery NormalizedQuery { get; set; }
        public string ApplicationName { get; set; }
        public string DatabaseName { get; set; }
        public string LoginName { get; set; }
        public string HostName { get; set; }
        public long AvgCpuUs { get; set; }
	    public long MinCpuUs { get; set; } 
	    public long MaxCpuUs { get; set; } 
	    public long SumCpuUs { get; set; } 
	    public long AvgReads { get; set; } 
	    public long MinReads { get; set; } 
	    public long MaxReads { get; set; } 
	    public long SumReads { get; set; } 
	    public long AvgWrites { get; set; } 
	    public long MinWrites { get; set; } 
	    public long MaxWrites { get; set; } 
	    public long SumWrites { get; set; } 
	    public long AvgDurationUs { get; set; } 
	    public long MinDurationUs { get; set; } 
	    public long MaxDurationUs { get; set; } 
	    public long SumDurationUs { get; set; } 
	    public long ExecutionCount { get; set; }
    }

}
