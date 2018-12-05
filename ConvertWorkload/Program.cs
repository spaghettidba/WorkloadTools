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

            EventReader reader = null;
            if (options.SourceFile.EndsWith(".trc"))
            {
                reader = new SqlTraceEventReader(options.SourceFile);
            }
            else if (options.SourceFile.EndsWith(".evt"))
            {
                // TODO: implement extended events reader
            }
            EventWriter writer = new WorkloadFileEventWriter(options.DestinationFile);
            WorkloadConverter converter = new WorkloadConverter(reader, writer);

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                logger.Info("Received shutdown signal...");
                source.CancelAfter(TimeSpan.FromSeconds(10)); // give a 10 seconds cancellation grace period 
                converter.Stop();
            };

            Task t = processConverter(converter);
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

        [Option('S', "Source", HelpText = "Source file")]
        public string SourceFile { get; set; }

        [Option('D', "Destination", HelpText = "Destination file")]
        public string DestinationFile { get; set; }

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
