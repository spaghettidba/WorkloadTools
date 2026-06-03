using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools;
using Newtonsoft.Json;
using System.IO;
using DouglasCrockford.JsMin;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Consumer.Replay;
using WorkloadTools.Consumer.Analysis;
using WorkloadTools.Util;

namespace WorkloadTools.Config
{
    public class SqlWorkloadConfig
    {
        public SqlWorkloadConfig()
        {
        }

        public WorkloadController Controller { get; set; }

        public static SqlWorkloadConfig LoadFromFile(string path)
        {
            var settings = new SqlWorkloadJsonSettings();
            using (var r = new StreamReader(path))
            {
                var json = r.ReadToEnd();
                var minifier = new JsMinifier();
                // minify JSON to strip away comments
                // Comments in config files are very useful but JSON parsers
                // do not allow comments. Minification solves the issue.
                SqlWorkloadConfig result = null;
                string jsonMin = null;
                try
                {
                    jsonMin = minifier.Minify(json);
                }
                catch (Exception e)
                {
                    throw new FormatException($"Unable to load configuration from '{path}'. The file contains syntax errors.", e);
                }

                try
                {
                    result = JsonConvert.DeserializeObject<SqlWorkloadConfig>(jsonMin, settings);
                }
                catch (Exception e)
                {
                    throw new FormatException($"Unable to load configuration from '{path}'. The file contains semantic errors.", e);
                }
                return result;
            }
        }

        public static void Test()
        {
            var settings = new SqlWorkloadJsonSettings();
            var x = new SqlWorkloadConfig()
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

            x.Controller.Consumers.Add(new ReplayConsumer()
            {
                ConnectionInfo = new SqlConnectionInfo()
                {
                    ServerName = "SQLDEMO\\SQL2016",
                    UserName = "sa",
                    Password = "P4$$w0rd!",
                    DatabaseName = "RTR",
                    SchemaName = "baseline"
                },
                DatabaseMap = new Dictionary<string, string>()
                {
                    { "DatabaseA", "DatabaseB" },
                    { "DatabaseC", "DatabaseD" }
                }
            });

            var s = JsonConvert.SerializeObject(x, settings);

            Console.WriteLine(s);

            //SqlWorkloadConfig tc = JsonConvert.DeserializeObject<SqlWorkloadConfig>(Samples.Sample.ToString(), settings);

        }

    }
}
