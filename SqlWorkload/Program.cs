using CommandLine;
using CommandLine.Text;
using NLog;
using NLog.Targets;
using System.Diagnostics;
using System.Runtime;
using WorkloadTools;
using WorkloadTools.Config;

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

            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            var fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            var version = fvi.FileMajorPart.ToString() + "." + fvi.FileMinorPart.ToString() + "." + fvi.FileBuildPart.ToString();
            var name = assembly.FullName;
            logger.Info(name + " " + version);

            try
            {
                var options = new Options();
                var result = Parser.Default.ParseArguments<Options>(args);
                result
                  .WithParsed(parsedOptions => options = parsedOptions)
                  .WithNotParsed(errors =>
                  {
                      foreach (var error in errors)
                      {
                          logger.Error(error.ToString());
                      }
                      var helpText = HelpText.AutoBuild(result, h =>
                      {
                          h.AdditionalNewLineAfterOption = false;
                          return h;
                      }, e => e);
                      Console.WriteLine(helpText);
                      Environment.Exit(1);
                  });
                Run(options, result);
            }
            catch(Exception e)
            {
                logger.Error(e);
            }

        }

        static void Run(Options options, ParserResult<Options> parseResult)
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

            options.ConfigurationFile = Path.GetFullPath(options.ConfigurationFile);
            logger.Info(String.Format("Reading configuration from '{0}'", options.ConfigurationFile));

            if (!File.Exists(options.ConfigurationFile))
            {
                logger.Error("File not found!");
                Console.WriteLine(options.GetUsage(parseResult));
                return;
            }

            var config = SqlWorkloadConfig.LoadFromFile(options.ConfigurationFile);
            config.Controller.Listener.Source = Path.GetFullPath(config.Controller.Listener.Source);

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(100)); // give a 100 seconds cancellation grace period 
                config.Controller.Stop();
            };

            var t = processController(config.Controller);
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
        [Option('F', "File", Default = "SqlWorkload.json", HelpText = "Configuration file")]
        public string ConfigurationFile { get; set; }

        [Option('L', "Log", HelpText = "Log file")]
        public string LogFile { get; set; }

        [Option('E', "LogLevel", HelpText = "Log level")]
        public string LogLevel { get; set; }

        public string GetUsage(ParserResult<Options> parseResult)
        {
            var help = HelpText.AutoBuild(parseResult, h =>
            {
                h.AdditionalNewLineAfterOption = false;
                return h;
            }, e => e);
            return help;
        }

    }
}
