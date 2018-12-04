using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConvertWorkload
{
    public class WorkloadConverter
    {
        private EventReader reader;
        private EventWriter writer;

        public WorkloadConverter(EventReader reader, EventWriter writer)
        {
            this.reader = reader;
            this.writer = writer;
        }

        public void Convert()
        {
            while (reader.HasMoreElements())
            {
                writer.Write(reader.Read());
            }
        }

        public void Stop()
        {
            reader.Dispose();
            writer.Dispose();
        }
    }
}
