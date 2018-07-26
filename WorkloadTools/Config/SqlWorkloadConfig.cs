using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools;
using System.Web.Script.Serialization;
using System.IO;
using DouglasCrockford.JsMin;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Consumer.Replay;
using WorkloadTools.Consumer.Analysis;

namespace WorkloadTools.Config
{
    public class SqlWorkloadConfig
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        public SqlWorkloadConfig()
        {
        }

        public WorkloadController Controller { get; set; }

        public static SqlWorkloadConfig LoadFromFile(string path)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer(new SqlWorkloadConfigTypeResolver());
            using (StreamReader r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                var minifier = new JsMinifier();
                // minify JSON to strip away comments
                // Comments in config files are very useful but JSON parsers
                // do not allow comments. Minification solves the issue.
                string jsonMin = minifier.Minify(json);
                return ser.Deserialize<SqlWorkloadConfig>(jsonMin);
            }
        }

        public static void Test()
        {
            JavaScriptSerializer ser = new JavaScriptSerializer(new SqlWorkloadConfigTypeResolver());
            SqlWorkloadConfig x = new SqlWorkloadConfig()
            {
                Controller = new WorkloadController()
            };
            x.Controller.Listener = new ExtendedEventsWorkloadListener()
            {
                Source = "Listener\\ExtendedEvents\\sqlworkload.sql",
                ConnectionInfo = new SqlConnectionInfo()
                {
                    ServerName = "SQLDEMO\\SQL2014",
                    UserName = "sa",
                    Password = "P4$$w0rd!"
                }
            };
            //x.Controller.Listener.Filter.DatabaseFilter.PredicateValue = "DS3";

            x.Controller.Consumers.Add(new ReplayConsumer()
            {
                ConnectionInfo = new SqlConnectionInfo()
                {
                    ServerName = "SQLDEMO\\SQL2016",
                    UserName = "sa",
                    Password = "P4$$w0rd!"
                }
            });

            x.Controller.Consumers.Add(new AnalysisConsumer()
            {
                ConnectionInfo = new SqlConnectionInfo()
                {
                    ServerName = "SQLDEMO\\SQL2016",
                    UserName = "sa",
                    Password = "P4$$w0rd!",
                    DatabaseName = "RTR",
                    SchemaName = "baseline"
                },
                UploadIntervalSeconds = 60
            });

            string s = ser.Serialize(x);

            Console.WriteLine(s);

            //SqlWorkloadConfig tc = ser.Deserialize<SqlWorkloadConfig>(Samples.Sample.ToString());

        }

    }
}
