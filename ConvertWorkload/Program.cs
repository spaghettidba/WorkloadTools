using CommandLine;
using CommandLine.Text;
using NLog;
using NLog.Targets;
using System.Diagnostics;
using System.Runtime;

namespace ConvertWorkload
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
                Run(options);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static void Run(Options options)
        {
            // reconfigure loggers to use a file in the current directory
            // or the file specified by the "Log" commandline parameter
            if(LogManager.Configuration != null)
            {
                var target = (FileTarget)LogManager.Configuration.FindTargetByName("logfile");
                if (target != null)
                {
                    var pathToLog = options.LogFile;
                    if (pathToLog == null)
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, "ConvertWorkload.log");
                    }
                    if (!Path.IsPathRooted(pathToLog))
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, pathToLog);
                    }
                    target.FileName = pathToLog;
                    LogManager.ReconfigExistingLoggers();
                }
            }

            // check whether localdb is installed
            logger.Info("Checking LocalDB...");
            var manager = new LocalDBManager();
            if (!manager.CanConnectToLocalDB())
            {
                logger.Info("Installing LocalDB...");
                try
                {
                    manager.InstallLocalDB();
                }
                catch (InvalidOperationException)
                {
                    logger.Error("This operation requires elevation. Restart the application as an administrator.");
                    return;
                }
            }

            EventReader reader = null;
            if (options.InputFile.EndsWith(".trc"))
            {
                reader = new SqlTraceEventReader(options.InputFile);
            }
            else 
            {
                reader = new ExtendedEventsEventReader(options.InputFile);
            }
            EventWriter writer = new WorkloadFileEventWriter(options.OutputFile);
            var converter = new WorkloadConverter(reader, writer);
            if(options.ApplicationFilter != null)
            {
                converter.ApplicationFilter = new string[1] { options.ApplicationFilter };
            }

            if (options.DatabaseFilter != null)
            {
                converter.DatabaseFilter = new string[1] { options.DatabaseFilter };
            }

            if (options.HostFilter != null)
            {
                converter.HostFilter = new string[1] { options.HostFilter };
            }

            if (options.LoginFilter != null)
            {
                converter.LoginFilter = new string[1] { options.LoginFilter };
            }

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(10)); // give a 10 seconds cancellation grace period 
                converter.Stop();
            };

            var t = processConverter(converter);
            t.Wait();
            logger.Info("Converter stopped.");
        }

        public static void CancelNotification()
        {
            logger.Info("Shutdown complete.");
        }

        public static async Task processConverter(WorkloadConverter converter)
        {
            source = new CancellationTokenSource();
            source.Token.Register(CancelNotification);
            var completionSource = new TaskCompletionSource<object>();
            source.Token.Register(() => completionSource.TrySetCanceled());
            var task = Task.Factory.StartNew(() => converter.Convert(), source.Token);
            await Task.WhenAny(task, completionSource.Task);
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
        [Option('L', "Log", HelpText = "Log file")]
        public string LogFile { get; set; }

        [Option('I', "Input", HelpText = "Input file", Required = true)]
        public string InputFile { get; set; }

        [Option('O', "Output", HelpText = "Output file", Required = true)]
        public string OutputFile { get; set; }

        [Option('A', "ApplicationFilter", HelpText = "Application filter")]
        public string ApplicationFilter { get; set; }

        [Option('D', "DatabaseFilter", HelpText = "Database filter")]
        public string DatabaseFilter { get; set; }

        [Option('H', "HostFilter", HelpText = "Host Filter")]
        public string HostFilter { get; set; }

        [Option('U', "LoginFilter", HelpText = "Login Filter")]
        public string LoginFilter { get; set; }
    }
}
