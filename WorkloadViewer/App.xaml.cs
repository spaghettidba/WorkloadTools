using CommandLine;
using CommandLine.Text;
using NLog;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;

namespace WorkloadViewer
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public Options Options { get; private set; }

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            Options = new Options();
            bool optionsAreGood = CommandLine.Parser.Default.ParseArguments(e.Args, Options);

            if (!optionsAreGood)
            {
                MessageBox.Show(Options.GetUsage());
                Shutdown();
            }

            // reconfigure loggers to use a file in the current directory
            // or the file specified by the "Log" commandline parameter
            if (LogManager.Configuration != null)
            {
                var target = (FileTarget)LogManager.Configuration.FindTargetByName("logfile");
                if (target != null)
                {
                    var pathToLog = Options.LogFile;
                    if (pathToLog == null)
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, "WorkloadViewer.log");
                    }
                    if (!Path.IsPathRooted(pathToLog))
                    {
                        pathToLog = Path.Combine(Environment.CurrentDirectory, pathToLog);
                    }
                    Console.WriteLine($"Writing logs to {pathToLog}");
                    target.FileName = pathToLog;
                    LogManager.ReconfigExistingLoggers();
                }
                else
                {
                    Console.WriteLine($"No file targets configured");
                }
            }
            else
            {
                Console.WriteLine($"NLog not configured");
            }
            logger.Info("Starting application");
        }

    }

    public class Options
    {
        [Option('F', "File", HelpText = "Configuration file")]
        public string ConfigurationFile { get; set; }

        [Option('L', "Log", HelpText = "Log File")]
        public string LogFile { get; set; }

        [Option('S', "BaselineServer", HelpText = "Baseline Server")]
        public string BaselineServer { get; set; }

        [Option('D', "BaselineDatabase", HelpText = "Baseline Database")]
        public string BaselineDatabase { get; set; }

        [Option('M', "BaselineSchema", HelpText = "Baseline Schema")]
        public string BaselineSchema { get; set; }

        [Option('U', "BaselineUsername", HelpText = "Baseline Username")]
        public string BaselineUsername { get; set; }

        [Option('P', "BaselinePassword", HelpText = "Baseline Password")]
        public string BaselinePassword { get; set; }

        [Option('T', "BenchmarkServer", HelpText = "Benchmark Server")]
        public string BenchmarkServer { get; set; }

        [Option('E', "BenchmarkDatabase", HelpText = "Benchmark Database")]
        public string BenchmarkDatabase { get; set; }

        [Option('N', "BenchmarkSchema", HelpText = "Benchmark Schema")]
        public string BenchmarkSchema { get; set; }

        [Option('V', "BenchmarkUsername", HelpText = "Benchmark Username")]
        public string BenchmarkUsername { get; set; }

        [Option('Q', "BenchmarkPassword", HelpText = "Benchmark Password")]
        public string BenchmarkPassword { get; set; }

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
