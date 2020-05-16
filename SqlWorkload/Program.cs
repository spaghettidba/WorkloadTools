using CommandLine;
using CommandLine.Text;
using NLog;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Config;
using WorkloadTools.Consumer;
using WorkloadTools.Listener;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Listener.Trace;

namespace SqlWorkload
{
    class Program
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();
        private static CancellationTokenSource source;

        static void Main(string[] args)
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
            catch(Exception e)
            {
                logger.Error(e);
            }

        }

        static void Run(Options options)
        {
            // reconfigure loggers to use a file in the current directory
            // or the file specified by the "Log" commandline parameter
            if (LogManager.Configuration != null)
            {
                var target = (FileTarget)LogManager.Configuration.FindTargetByName("logfile");
                if (target != null)
                {
                    var pathToLog = options.LogFile;
                    if (pathToLog == null)
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, "SqlWorkload.log");
                    }
                    if (!Path.IsPathRooted(pathToLog))
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, pathToLog);
                    }
                    target.FileName = pathToLog;

                    if(options.LogLevel != null)
                    {
                        foreach(var rule in LogManager.Configuration.LoggingRules)
                        {
                            foreach (var level in LogLevel.AllLoggingLevels)
                            {
                                rule.DisableLoggingForLevel(level);
                            }
                            rule.EnableLoggingForLevels(LogLevel.FromString(options.LogLevel),LogLevel.Fatal);
                        }
                    }

                    LogManager.ReconfigExistingLoggers();
                }
            }

            options.ConfigurationFile = System.IO.Path.GetFullPath(options.ConfigurationFile);
            logger.Info(String.Format("Reading configuration from '{0}'", options.ConfigurationFile));

            SqlWorkloadConfig config = SqlWorkloadConfig.LoadFromFile(options.ConfigurationFile);
            config.Controller.Listener.Source = System.IO.Path.GetFullPath(config.Controller.Listener.Source);

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(100)); // give a 100 seconds cancellation grace period 
                config.Controller.Stop();
            };

            Task t = processController(config.Controller);
            t.Wait();
            logger.Info("Controller stopped.");
            config.Controller.Dispose();
            logger.Info("Controller disposed.");
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





    class Options
    {
        [Option('F', "File", DefaultValue = "SqlWorkload.json", HelpText = "Configuration file")]
        public string ConfigurationFile { get; set; }

        [Option('L', "Log", HelpText = "Log file")]
        public string LogFile { get; set; }

        [Option('E', "LogLevel", HelpText = "Log level")]
        public string LogLevel { get; set; }

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
