using CommandLine;
using CommandLine.Text;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Consumer;
using WorkloadTools.Listener;

namespace SqlWorkload
{
    class Program
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(GenericErrorHandler);

            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            string version = fvi.FileMajorPart.ToString() + "." + fvi.FileMinorPart.ToString() + "." + fvi.FileBuildPart.ToString();
            string name = assembly.FullName;
            logger.Info(name + " " + version);

            WorkloadListener listener = null;
            WorkloadController controller = null;

            var options = new Options();
            if (options.ListenerType.ToLower() == "ProfilerWorkloadListener".ToLower())
            {
                listener = new ProfilerWorkloadListener();
                options.TraceDefinition = System.IO.Path.GetFullPath(options.TraceDefinition);
                listener.Source = options.TraceDefinition;
            }
            else if (options.ListenerType.ToLower() == "SqlTraceWorkloadListener".ToLower())
            {
                listener = new SqlTraceWorkloadListener();
                options.SqlTraceScript = System.IO.Path.GetFullPath(options.SqlTraceScript);
                listener.Source = options.SqlTraceScript;
            }
            else if (options.ListenerType.ToLower() == "ExtendedEventsWorkloadListener".ToLower())
            {
                listener = new ExtendedEventsWorkloadListener();
                options.XESessionDefinition = System.IO.Path.GetFullPath(options.XESessionDefinition);
                listener.Source = options.XESessionDefinition;
            }
            else
            {
                throw new ArgumentOutOfRangeException("The Listener has to be a registered Listener type");
            }


            listener.ConnectionInfo = new SqlConnectionInfo()
            {
                ServerName = options.SourceServerName,
                DatabaseName = "master",
                UserName = options.SourceUserName,
                Password = options.SourcePassword,
            };
            

            listener.Filter = new WorkloadEventFilter()
            {
                DatabaseFilter = options.DatabaseFilter,
                ApplicationFilter = options.ApplicationFilter,
                HostFilter = options.HostFilter,
                LoginFilter = options.LoginFilter
            };


            controller = new WorkloadController(listener);

            // Register the Replay Consumer
            if(!String.IsNullOrEmpty(options.TargetServerName))
            {
                controller.RegisterConsumer(new ReplayConsumer()
                {
                    ConnectionInfo = new SqlConnectionInfo()
                    {
                        ServerName = options.TargetServerName,
                        DatabaseName = "master",
                        UserName = options.TargetUserName,
                        Password = options.TargetPassword
                    }
                });
            }

            // Register the Analysis Consumer
            if (!String.IsNullOrEmpty(options.StatsServer))
            {
                controller.RegisterConsumer(new AnalysisConsumer()
                {
                    ConnectionInfo = new SqlConnectionInfo()
                    {
                        ServerName = options.StatsServer,
                        DatabaseName = options.StatsDatabase,
                        SchemaName = options.StatsSchema,
                        UserName = options.StatsUserName,
                        Password = options.StatsPassword
                    },
                    UploadIntervalSeconds = options.StatsInterval
                });
                
            }


            Task t = controller.Start();
            t.Wait(); //TODO: Add CancellationToken

        }



        static void GenericErrorHandler(object sender, UnhandledExceptionEventArgs e)
        {
            try
            {
                logger.Error(e.ToString());
            }
            finally
            {
                Console.WriteLine("Caught unhandled exception...");

            }
        }

    }


    class Options
    {
        [Option("ListenerType", HelpText = "Class name of the Listener")]
        public string ListenerType { get; set; }

        [Option("TraceDefinition", DefaultValue = "sqlworkload.tdf", HelpText = "Path to the Trace Definition file")]
        public string TraceDefinition { get; set; }

        [Option("SqlTraceScript", DefaultValue = "sqlworkload.sql", HelpText = "Path to the Sql Trace script file")]
        public string SqlTraceScript { get; set; }

        [Option("XESessionDefinition", DefaultValue = "xeworkload.sql", HelpText = "Path to the XE Session Definition file")]
        public string XESessionDefinition { get; set; }

        [Option('S', "SourceServerName", DefaultValue = ".", HelpText = "Source SQL Server instance")]
        public string SourceServerName { get; set; }

        [Option('U', "SourceUserName", HelpText = "Source User Name")]
        public string SourceUserName { get; set; }

        [Option('P', "SourcePassword", HelpText = "Source Password")]
        public string SourcePassword { get; set; }

        [Option('T', "TargetServerName", DefaultValue = ".", HelpText = "Target SQL Server instance")]
        public string TargetServerName { get; set; }

        [Option('V', "TargetUserName", HelpText = "Target User Name")]
        public string TargetUserName { get; set; }

        [Option('Q', "TargetPassword", HelpText = "Target Password")]
        public string TargetPassword { get; set; }

        [Option('A', "ApplicationFilter", HelpText = "Application Name filter. Accepts comma separated lists.")]
        public string ApplicationFilter { get; set; }

        [Option('D', "DatabaseFilter", HelpText = "Database Name filter. Accepts comma separated lists.")]
        public string DatabaseFilter { get; set; }

        [Option('H', "HostFilter", HelpText = "Host Name filter. Accepts comma separated lists.")]
        public string HostFilter { get; set; }

        [Option('L', "LoginFilter", HelpText = "Login Name filter. Accepts comma separated lists.")]
        public string LoginFilter { get; set; }

        [Option("StatsServer", HelpText = "SQL Server instance for workload statistics output")]
        public string StatsServer { get; set; }

        [Option("StatsDatabase", HelpText = "Database stats output")]
        public string StatsDatabase { get; set; }

        [Option("StatsUserName", HelpText = "UserName for stats output")]
        public string StatsUserName { get; set; }

        [Option("StatsPassword", HelpText = "Password for stats output")]
        public string StatsPassword { get; set; }

        [Option("StatsSchema", DefaultValue = "dbo", HelpText = "Schema name for stats output")]
        public string StatsSchema { get; set; }

        [Option("StatsInterval", HelpText = "Interval, in minutes, for stats output")]
        public int StatsInterval { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }

}
