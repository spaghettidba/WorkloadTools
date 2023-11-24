using WorkloadViewer.Model;

namespace WorkloadViewer.ViewModel
{
    public class QueryResult
    {
        public long query_hash { get; set; }
        
        public string query_text { get; set; }
        
        public string query_normalized { get; set; }
        
        public long sum_duration_us { get; set; }
        
        public double avg_duration_us { get; set; }
        
        public long sum_cpu_us { get; set; }
        
        public double avg_cpu_us { get; set; }
        
        public long sum_reads { get; set; }
        
        public double avg_reads { get; set; }
        
        public long execution_count { get; set; }
        
        public long sum_duration_us2 { get; set; }
        
        public long diff_sum_duration_us { get; set; }
        
        public double avg_duration_us2 { get; set; }
        
        public double diff_avg_duration_us { get; set; }
        
        public long sum_cpu_us2 { get; set; }
        
        public long diff_sum_cpu_us { get; set; }
        
        public double avg_cpu_us2 { get; set; }
        
        public double diff_avg_cpu_us { get; set; }
        
        public long sum_reads2 { get; set; }
        
        public long diff_sum_reads { get; set; }
        
        public double avg_reads2 { get; set; }
        
        public double diff_avg_reads { get; set; }
        
        public long execution_count2 { get; set; }
        
        public long diff_execution_count { get; set; }
        
        public QueryDetails querydetails { get; set; }
        
        public ICSharpCode.AvalonEdit.Document.TextDocument document { get; set; }
    }
}