using WorkloadTools;
using WorkloadTools.Consumer.WorkloadFile;

namespace ConvertWorkload
{
    public class WorkloadFileEventWriter : EventWriter
    {
        private WorkloadFileWriterConsumer consumer;

        public WorkloadFileEventWriter(string outputPath)
        {
            consumer = new WorkloadFileWriterConsumer()
            {
                OutputFile = outputPath
            };
        }

        public override void Write(WorkloadEvent evt)
        {
            consumer.Consume(evt);
        }

        protected override void Dispose(bool disposing)
        {
            consumer.Dispose();
        }
    }
}
