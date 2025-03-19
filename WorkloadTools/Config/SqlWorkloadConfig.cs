using DouglasCrockford.JsMin;
using Newtonsoft.Json;
using NLog;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Consumer.Replay;
using XESmartTarget.Core.Utils;

namespace WorkloadTools.Config
{
    public class SqlWorkloadConfig
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();
        public SqlWorkloadConfig()
        {
        }

        public WorkloadController Controller { get; set; }

        public static SqlWorkloadConfig LoadFromFile(string path)
        {
            using (var r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                var minifier = new JsMinifier();
                string jsonMin;
                try
                {
                    jsonMin = minifier.Minify(json);
                }
                catch (Exception e)
                {
                    throw new FormatException($"Unable to load configuration from '{path}'. " +
                                              "The file contains syntax errors.", e);
                }

                // Deserializza in un dizionario generico
                Dictionary<string, object> dictionary;
                try
                {
                    dictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonMin);
                }
                catch (Exception e)
                {
                    throw new FormatException($"Unable to load configuration from '{path}'. The file contains semantic errors (invalid JSON).", e);
                }

                ModelConverter converter = new ModelConverter();
                SqlWorkloadConfig result;
                try
                {
                    result = (SqlWorkloadConfig)converter.Deserialize(dictionary, typeof(SqlWorkloadConfig));
                }
                catch (Exception e)
                {
                    throw new FormatException($"Unable to convert dictionary to SqlWorkloadConfig.", e);
                }
                return result;
            }
        }

        public static void Test()
        {
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

            string s = JsonConvert.SerializeObject(x, Formatting.Indented);
            Console.WriteLine(s);
        }
    }
}
