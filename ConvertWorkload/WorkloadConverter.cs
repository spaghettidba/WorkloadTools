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
        private bool stopped = false;

        public string[] ApplicationFilter { get; set; }
        public string[] DatabaseFilter { get; set; }
        public string[] HostFilter { get; set; }
        public string[] LoginFilter { get; set; }

        public WorkloadConverter(EventReader reader, EventWriter writer)
        {
            this.reader = reader;
            this.writer = writer;
        }

        public void Convert()
        {
            try
            {
                reader.ApplicationFilter = new string[1] { "" };
                reader.DatabaseFilter = new string[1] { "" };
                reader.HostFilter = new string[1] { "" };
                reader.LoginFilter = new string[1] { "" };
                if (ApplicationFilter != null) reader.ApplicationFilter = ApplicationFilter;
                if (DatabaseFilter != null) reader.DatabaseFilter = DatabaseFilter;
                if (HostFilter != null) reader.HostFilter = HostFilter;
                if (LoginFilter != null) reader.LoginFilter = LoginFilter;

                while (!reader.HasFinished() && !stopped)
                {
                    writer.Write(reader.Read());
                }
            }
            catch(Exception ex)
            {
                stopped = true;
                logger.Error(ex);
            }
        }

        public void Stop()
        {
            stopped = true;
            reader.Dispose();
            writer.Dispose();
        }
    }
}
