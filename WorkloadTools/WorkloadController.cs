using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer;
using WorkloadTools.Listener.ExtendedEvents.DBStreamReader;

namespace WorkloadTools
{
    public class WorkloadController
    {
       
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public static String BaseLocation = new Uri(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().CodeBase)).LocalPath;


        public WorkloadListener Listener { get; set; }
        public List<WorkloadConsumer> Consumers = new List<WorkloadConsumer>();
        
        public int AutoInsertTestDataClientNumber { get; set; }
        public DatabaseFactory.DatabaseProvider DatabaseProvider { get; set; }


        private bool stopped = false;
        private bool disposed = false;
        private const int MAX_DISPOSE_TIMEOUT_SECONDS = 5;


        public WorkloadController()
        {
        }

        public void Run()
        {

            try
            {
                DatabaseFactory.SetProvider(DatabaseProvider);
                Listener.Initialize();
                if (AutoInsertTestDataClientNumber > 0)
                {
                    for (int client = 0; client < AutoInsertTestDataClientNumber; client++)
                    {
                        Task.Delay(2000).ContinueWith((t) =>
                        {
                            Listener.ConnectionInfo.DatabaseName = Listener.DatabaseFilter;
                            using (SqlConnection cns = new SqlConnection(Listener.ConnectionInfo.ConnectionString))
                            {
                                using (SqlCommand cmd = new SqlCommand())
                                {
                                    cmd.Connection = cns;
                                    cmd.CommandText = "INSERT INTO TB1(VAL) VALUES('ooo')";
                                    cns.Open();
                                    while (!stopped)
                                    {
                                        cmd.ExecuteNonQuery();
                                        Thread.Sleep(1);
                                    }
                                }
                            }
                        });
                    }
                }
                
                Task.Delay(2000).ContinueWith((t) =>
                {
                    while (!stopped)
                    {
                        (DateTime, long) result = DatabaseFactory.Current.GetLastInsertInfo();
                        logger.Info($"{DateTime.Now} - LastRecv:{result.Item1} - Evt readed:{Listener.GetEventReaded()} - Evt tot:{result.Item2}");
                        Thread.Sleep(1000);                        
                    }
                });

                while (!stopped)
                {
                    if (!Listener.IsRunning)
                        Stop();

                    var evt = Listener.Read();
                    if (evt == null)
                        continue;
                    Parallel.ForEach(Consumers, (cons) =>
                    {
                        cons.Consume(evt);
                    });
                }
                if (!disposed)
                {
                    disposed = true;
                    Listener.Dispose();
                    foreach (var cons in Consumers)
                    {
                        cons.Dispose();
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("Uncaught Exception");
                logger.Error(e.Message);
                logger.Error(e.StackTrace);

                Exception ex = e;
                while ((ex = ex.InnerException) != null){
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }
            }
        }

        public Task Start()
        {
            return Task.Factory.StartNew(() => Run());
        }

        public void Stop()
        {
            stopped = true;
            int timeout = 0;
            while(!disposed && timeout < (MAX_DISPOSE_TIMEOUT_SECONDS * 1000))
            {
                Thread.Sleep(100);
                timeout += 100;
            }
            if (!disposed)
            {
                disposed = true;
                if(Listener != null)
                    Listener.Dispose();

                foreach (var cons in Consumers)
                {
                    if(cons != null)
                        cons.Dispose();
                }
            }
        }

    }
}
