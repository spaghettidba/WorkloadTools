using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools;

namespace ConvertWorkload
{
    public class WorkloadConverter
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private EventReader reader;
        private EventWriter writer;

        public string ApplicationFilter { get; set; }
        public string DatabaseFilter { get; set; }
        public string HostFilter { get; set; }
        public string LoginFilter { get; set; }

        public WorkloadConverter(EventReader reader, EventWriter writer)
        {
            this.reader = reader;
            this.writer = writer;
        }

        public void Convert()
        {
            try
            {
                reader.ApplicationFilter = "";
                reader.DatabaseFilter = "";
                reader.HostFilter = "";
                reader.LoginFilter = "";
                if (ApplicationFilter != null) reader.ApplicationFilter = ApplicationFilter;
                if (DatabaseFilter != null) reader.DatabaseFilter = DatabaseFilter;
                if (HostFilter != null) reader.HostFilter = HostFilter;
                if (LoginFilter != null) reader.LoginFilter = LoginFilter;

                while (!reader.HasFinished())
                {
                    writer.Write(reader.Read());
                }
            }
            catch(Exception ex)
            {
                logger.Error(ex);
            }
        }

        public void Stop()
        {
            reader.Dispose();
            writer.Dispose();
        }
    }
}
