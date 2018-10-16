using CommandLine;
using CommandLine.Text;
using NLog;
using System;
using System.Diagnostics;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Config;
using WorkloadTools.Listener.ExtendedEvents.DBStreamReader;

namespace SqlWorkload
{
    internal class Program
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        private static CancellationTokenSource source;

        private static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(GenericErrorHandler);
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            string version = fvi.FileMajorPart.ToString() + "." + fvi.FileMinorPart.ToString() + "." + fvi.FileBuildPart.ToString();
            string name = assembly.FullName;
            logger.Info(name + " " + version);

            try
            {
                var options = new Options();
                if (!CommandLine.Parser.Default.ParseArguments(args, options))
                {
                    return;
                }
                Run(options);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static void Run(Options options)
        {
            options.ConfigurationFile = System.IO.Path.GetFullPath(options.ConfigurationFile);
            logger.Info(String.Format("Reading configuration from '{0}'", options.ConfigurationFile));
            logger.Info(String.Format("AutoInsertData '{0}'", options.AutoInsertTestDataClientNumber));
            logger.Info(String.Format("Listener database '{0}'", options.ListenerDbProvider));

            SqlWorkloadConfig config = SqlWorkloadConfig.LoadFromFile(options.ConfigurationFile);
            config.Controller.Listener.Source = System.IO.Path.GetFullPath(config.Controller.Listener.Source);
            config.Controller.AutoInsertTestDataClientNumber = options.AutoInsertTestDataClientNumber;
            config.Controller.DatabaseProvider = options.ListenerDbProvider;
            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(10)); // give a 10 seconds cancellation grace period
                config.Controller.Stop();
            };

            Task t = processController(config.Controller);
            t.Wait();
            logger.Info("Controller stopped.");
        }

        private static void GenericErrorHandler(object sender, UnhandledExceptionEventArgs e)
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

        public static async Task processController(WorkloadController controller)
        {
            source = new CancellationTokenSource();
            source.Token.Register(CancelNotification);
            var completionSource = new TaskCompletionSource<object>();
            source.Token.Register(() => completionSource.TrySetCanceled());
            var task = Task.Factory.StartNew(() => controller.Run(), source.Token);
            await Task.WhenAny(task, completionSource.Task);
        }

        public static void CancelNotification()
        {
            logger.Info("Shutdown complete.");
        }
    }

    internal class Options
    {
        [Option('F', "File", DefaultValue = "SqlWorkload.json", HelpText = "Configuration file")]
        public string ConfigurationFile { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [Option('A', "AutoInsertTestDataClientNumber")]
        public int AutoInsertTestDataClientNumber{ get; set; }

        [Option('D', "DatabaseProvider", DefaultValue =1, HelpText ="Database provider. 1-LiteDB, 2-Sqlite")]
        [ParserState]
        public DatabaseFactory.DatabaseProvider ListenerDbProvider { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}