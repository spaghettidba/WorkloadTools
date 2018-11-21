using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
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
        }

    }

    public class Options
    {
        [Option('F', "File", HelpText = "Configuration file")]
        public string ConfigurationFile { get; set; }

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
