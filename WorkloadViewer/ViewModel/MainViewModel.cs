using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using MahApps.Metro.Controls.Dialogs;
using OxyPlot;
using OxyPlot.Axes;
using OxyPlot.Series;
using System;
using System.Collections.Generic;
using System.Windows.Input;
using System.Linq;
using System.Diagnostics;
using WorkloadViewer.Model;
using System.Collections.ObjectModel;
using System.Data;
using System.Windows;
using GalaSoft.MvvmLight.Messaging;
using NLog;
using System.Threading.Tasks;
using System.Threading;

namespace WorkloadViewer.ViewModel
{

    public class MainViewModel : ViewModelBase
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        public bool CompareMode
        {
            get
            {
                return _benchmarkWorkloadAnalysis != null;
            }
        }

        public Visibility CompareModeVisibility
        {
            get
            {
                if(CompareMode) return Visibility.Visible;
                else return Visibility.Collapsed;
            }
        }

        internal Options _options;
        internal bool _invalidOptions = false;
        private WorkloadAnalysis _baselineWorkloadAnalysis;
        private WorkloadAnalysis _benchmarkWorkloadAnalysis;

        public string StatusMessage { get; set; }

        private PlotModel[] PlotModels = new PlotModel[3];

        public PlotModel CpuPlotModel { get; private set; }
        public PlotModel DurationPlotModel { get; private set; }
        public PlotModel BatchesPlotModel { get; private set; }


        public List<FilterDefinition> HostList { get; set; }
        public List<FilterDefinition> ApplicationList { get; set; }
        public List<FilterDefinition> DatabaseList { get; set; }
        public List<FilterDefinition> LoginList { get; set; }

        public ICommand LoadedCommand { get; set; }
        public ICommand RenderedCommand { get; set; }
        public ICommand KeyDownCommand { get; set; }
        public ICommand ApplyCommand { get; set; }

        public IEnumerable<object> Queries { get; private set; }

        public bool Initialized { get; private set; } = false;


        private IDialogCoordinator _dialogCoordinator;
        private DateTime _lastAxisAdjust = DateTime.Now;

        public MainViewModel()
        {
            LoadedCommand = new RelayCommand<EventArgs>(Loaded);
            RenderedCommand = new RelayCommand<EventArgs>(Rendered);
            KeyDownCommand = new RelayCommand<KeyEventArgs>(KeyDown);
            ApplyCommand = new RelayCommand<EventArgs>(ApplyFilters);
            _dialogCoordinator = DialogCoordinator.Instance;
            PlotModels = new PlotModel[3];
        }

        private void ApplyFilters(EventArgs obj)
        {
            InitializeCharts();
            InitializeQueries();
            RefreshAllCharts();
        }

        private void Rendered(EventArgs ev)
        {
            if (_invalidOptions)
            {
                ShowStatusMessage("ShowConnectionInfoDialog");
                ShowConnectionInfoDialog();
            }
            else
            {
                ShowStatusMessage("Initializing");
                InitializeAll();
                ShowStatusMessage("Initialized");
            }
        }

        private async void InitializeAll()
        {
            ProgressDialogController controller = await _dialogCoordinator.ShowProgressAsync(this, "Loading data", String.Empty, false);
            controller.SetIndeterminate();

            try
            {
                Initialized = false;
                await Task.Run(() =>
                    {
                        InitializeWorkloadAnalysis();
                        InitializeFilters();
                        InitializeCharts();
                        RefreshAllCharts();
                    });

                // This cannot be run async due to threading errors
                // in AvalonEdit.TextEditor
                // "TextDocument can be accessed only from the thread that owns it"
                InitializeQueries();
                Initialized = true;
            }
            catch (Exception e)
            {
                ShowStatusMessage($"Exception: {e.Message}");
                await _dialogCoordinator.ShowMessageAsync(this, "WorkloadViewer", "Unable to load data: " + e.Message);
                ShowConnectionInfoDialog();
            }
            finally
            {
                await controller.CloseAsync();
                while (controller.IsOpen)
                {
                    await controller.CloseAsync();
                    Thread.Sleep(5);
                }
            }
        }

        private async void ShowConnectionInfoDialog()
        {
            var editor = new View.ConnectionInfoDialog();
            var viewModel = new ConnectionInfoEditorViewModel() { Context = this, Dialog = editor };
            editor.DataContext = viewModel;
            viewModel.BaselineServer = _options.BaselineServer;
            viewModel.BaselineDatabase = _options.BaselineDatabase;
            viewModel.BaselineSchema = _options.BaselineSchema;
            viewModel.BaselineUsername = _options.BaselineUsername;
            viewModel.BaselinePassword = _options.BaselinePassword;
            viewModel.BenchmarkServer = _options.BenchmarkServer;
            viewModel.BenchmarkDatabase = _options.BenchmarkDatabase;
            viewModel.BenchmarkSchema = _options.BenchmarkSchema;
            viewModel.BenchmarkUsername = _options.BenchmarkUsername;
            viewModel.BenchmarkPassword = _options.BenchmarkPassword;
            await _dialogCoordinator.ShowMetroDialogAsync(this, editor);
        }


        public async void SetConnectionInfo(ConnectionInfoEditorViewModel viewModel)
        {
            _options.BaselineServer = viewModel.BaselineServer;
            _options.BaselineDatabase = viewModel.BaselineDatabase;
            _options.BaselineSchema = viewModel.BaselineSchema;
            _options.BaselineUsername = viewModel.BaselineUsername;
            _options.BaselinePassword = viewModel.BaselinePassword;

            _options.BenchmarkServer = viewModel.BenchmarkServer;
            _options.BenchmarkDatabase = viewModel.BenchmarkDatabase;
            _options.BenchmarkSchema = viewModel.BenchmarkSchema;
            _options.BenchmarkUsername = viewModel.BenchmarkUsername;
            _options.BenchmarkPassword = viewModel.BenchmarkPassword;

            _invalidOptions = false;

            ShowStatusMessage("Pre InitializeAll");
            // now that the options are filled, I can invoke the initialization
            InitializeAll();
            BaseMetroDialog showingDialog = null;
            showingDialog = await _dialogCoordinator.GetCurrentDialogAsync<BaseMetroDialog>(this);
            if(showingDialog != null)
            {
                await _dialogCoordinator.HideMetroDialogAsync(this, showingDialog);
            }
            
            ShowStatusMessage("Post InitializeAll");
        }


        private void Loaded(EventArgs ev)
        {
            if (!ParseOptions())
            {
                _invalidOptions = true;
            }
        }

        private void InitializeWorkloadAnalysis()
        {
            _baselineWorkloadAnalysis = new WorkloadAnalysis() { Name = "Baseline" };
            _baselineWorkloadAnalysis.ConnectionInfo = new SqlConnectionInfo()
            {
                ServerName = _options.BaselineServer,
                DatabaseName = _options.BaselineDatabase,
                SchemaName = _options.BaselineSchema,
                UserName = _options.BaselineUsername,
                Password = _options.BaselinePassword
            };
            _baselineWorkloadAnalysis.Load();

            if(_options.BenchmarkSchema != null)
            {
                _benchmarkWorkloadAnalysis = new WorkloadAnalysis() { Name = "Benchmark" };
                _benchmarkWorkloadAnalysis.ConnectionInfo = new SqlConnectionInfo()
                {
                    ServerName = _options.BenchmarkServer,
                    DatabaseName = _options.BenchmarkDatabase,
                    SchemaName = _options.BenchmarkSchema,
                    UserName = _options.BenchmarkUsername,
                    Password = _options.BenchmarkPassword
                };
                _benchmarkWorkloadAnalysis.Load();
            }
        }


        private void InitializeQueries()
        {
            // Initialize the queries
            logger.Info("Entering baseline evaluation");

            bool zoomIsSet = PlotModels[0].DefaultXAxis != null;

            double xstart = 0;
            double xend = 0;

            if (zoomIsSet)
            {
                xstart = PlotModels[0].DefaultXAxis.ActualMinimum;
                xend = PlotModels[0].DefaultXAxis.ActualMaximum;
                if (xstart < 0) xstart = 0;
            }

            var baseline = from t in _baselineWorkloadAnalysis.Points
                           where ApplicationList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.ApplicationName)
                                && HostList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.HostName)
                                && DatabaseList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.DatabaseName)
                                && LoginList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.LoginName)
                                && (!zoomIsSet || t.OffsetMinutes >= xstart )
                                && (!zoomIsSet || t.OffsetMinutes <= xend)
                           group t by new
                           {
                               query = t.NormalizedQuery
                           }
                           into grp
                           select new
                           {
                               query = grp.Key.query,
                               sum_duration_us = grp.Sum(t => t.SumDurationUs),
                               avg_duration_us = grp.Average(t => t.AvgDurationUs),
                               sum_cpu_us = grp.Sum(t => t.SumCpuUs),
                               avg_cpu_us = grp.Average(t => t.AvgCpuUs),
                               sum_reads = grp.Sum(t => t.SumReads),
                               avg_reads = grp.Average(t => t.AvgReads),
                               execution_count = grp.Sum(t => t.ExecutionCount)
                           };

            logger.Info("Baseline evaluation completed");
            logger.Info("Entering benchmark evaluation");

            var benchmark = from t in baseline where false select new { t.query, t.sum_duration_us, t.avg_duration_us, t.sum_cpu_us, t.avg_cpu_us, t.sum_reads, t.avg_reads, t.execution_count };

            if (_benchmarkWorkloadAnalysis != null)
            {
                benchmark = from t in _benchmarkWorkloadAnalysis.Points
                            where ApplicationList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.ApplicationName)
                                && HostList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.HostName)
                                && DatabaseList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.DatabaseName)
                                && LoginList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.LoginName)
                                && (!zoomIsSet || t.OffsetMinutes >= xstart)
                                && (!zoomIsSet || t.OffsetMinutes <= xend)
                            group t by new
                            {
                                query = t.NormalizedQuery
                            }
                            into grp
                            select new
                            {
                                query = grp.Key.query,
                                sum_duration_us = grp.Sum(t => t.SumDurationUs),
                                avg_duration_us = grp.Average(t => t.AvgDurationUs),
                                sum_cpu_us = grp.Sum(t => t.SumCpuUs),
                                avg_cpu_us = grp.Average(t => t.AvgCpuUs),
                                sum_reads = grp.Sum(t => t.SumReads),
                                avg_reads = grp.Average(t => t.AvgReads),
                                execution_count = grp.Sum(t => t.ExecutionCount)
                            };
            }

            logger.Info("Benchmark evaluation completed");
            logger.Info("Merging sets");

            var merged =
                from b in baseline
                join k in benchmark
                    on b.query.Hash equals k.query.Hash
                    into joinedData
                from j in joinedData.DefaultIfEmpty()
                select new
                {
                    query_hash = b.query.Hash,
                    query_text = b.query.ExampleText,
                    query_normalized = b.query.NormalizedText,
                    b.sum_duration_us,
                    b.avg_duration_us,
                    b.sum_cpu_us,
                    b.avg_cpu_us,
                    b.sum_reads,
                    b.avg_reads,
                    b.execution_count,
                    sum_duration_us2     = j == null ? 0 : j.sum_duration_us,
                    diff_sum_duration_us = j == null ? 0 : j.sum_duration_us - b.sum_duration_us,
                    avg_duration_us2     = j == null ? 0 : j.avg_duration_us,
                    sum_cpu_us2          = j == null ? 0 : j.sum_cpu_us,
                    diff_sum_cpu_us      = j == null ? 0 : j.sum_cpu_us - b.sum_cpu_us,
                    avg_cpu_us2          = j == null ? 0 : j.avg_cpu_us,
                    sum_reads2           = j == null ? 0 : j.sum_reads,
                    avg_reads2           = j == null ? 0 : j.avg_reads,
                    execution_count2     = j == null ? 0 : j.execution_count,
                    querydetails = new QueryDetails(b.query, _baselineWorkloadAnalysis, _benchmarkWorkloadAnalysis),
                    document = new ICSharpCode.AvalonEdit.Document.TextDocument() { Text = b.query.ExampleText }
                };


            Queries = merged;

            logger.Info("Sets merged");

            RaisePropertyChanged("Queries");
            RaisePropertyChanged("CompareModeVisibility");
            RaisePropertyChanged("CompareMode");

            string sortCol = CompareMode ? "diff_sum_duration_us" : "sum_duration_us";
            var msg = new SortColMessage(sortCol, System.ComponentModel.ListSortDirection.Descending);
            Messenger.Default.Send<SortColMessage>(msg);
        }


        private bool ParseOptions()
        {
            _options = ((WorkloadViewer.App)App.Current).Options;

            if(_options.ConfigurationFile != null)
            {
                // TODO: read configuration from file
            }
            else
            {
                if(_options.BaselineServer == null || _options.BaselineDatabase == null)
                {
                    return false;
                }
            }
            return true;
        }

        private void KeyDown(KeyEventArgs e)
        {
            if(e.Key == Key.F5)
            {
                // TODO: refreshing should keep zoom and filters
                InitializeAll();
            }
            if (e.Key == Key.F8)
            {
                ShowConnectionInfoDialog();
            }
        }


        private void RefreshAllCharts()
        {
            RaisePropertyChanged("CpuPlotModel");
            RaisePropertyChanged("DurationPlotModel");
            RaisePropertyChanged("BatchesPlotModel");
        }


        private void InitializeCharts()
        {
            CpuPlotModel = InitializePlotModel();
            CpuPlotModel.Axes[1].Title = "Cpu (us)";
            CpuPlotModel.Title = "Cpu";
            CpuPlotModel.Series.Add(LoadCpuSeries(_baselineWorkloadAnalysis, OxyColor.Parse("#01B8AA")));
            if(_options.BenchmarkSchema != null)
            {
                CpuPlotModel.Series.Add(LoadCpuSeries(_benchmarkWorkloadAnalysis, OxyColor.Parse("#000000")));
            }
            CpuPlotModel.PlotAreaBorderThickness = new OxyThickness(1,0,0,1);
            PlotModels[0] = CpuPlotModel;
            

            DurationPlotModel = InitializePlotModel();
            DurationPlotModel.Axes[1].Title = "Duration (us)";
            DurationPlotModel.Title = "Duration";
            DurationPlotModel.Series.Add(LoadDurationSeries(_baselineWorkloadAnalysis, OxyColor.Parse("#01B8AA")));
            if (_options.BenchmarkSchema != null)
            {
                DurationPlotModel.Series.Add(LoadDurationSeries(_benchmarkWorkloadAnalysis, OxyColor.Parse("#000000")));
            }
            DurationPlotModel.PlotAreaBorderThickness = new OxyThickness(1, 0, 0, 1);
            PlotModels[1] = DurationPlotModel;

            BatchesPlotModel = InitializePlotModel();
            BatchesPlotModel.Axes[1].Title = "Batches/second";
            BatchesPlotModel.Title = "Batches/second";
            BatchesPlotModel.Series.Add(LoadBatchesSeries(_baselineWorkloadAnalysis, OxyColor.Parse("#01B8AA")));
            if (_options.BenchmarkSchema != null)
            {
                BatchesPlotModel.Series.Add(LoadBatchesSeries(_benchmarkWorkloadAnalysis, OxyColor.Parse("#000000")));
            }
            BatchesPlotModel.PlotAreaBorderThickness = new OxyThickness(1, 0, 0, 1);
            PlotModels[2] = BatchesPlotModel;
        }


        private PlotModel InitializePlotModel()
        {
            PlotModel plotModel = new PlotModel();
            plotModel.LegendOrientation = LegendOrientation.Horizontal;
            plotModel.LegendPlacement = LegendPlacement.Inside;
            plotModel.LegendPosition = LegendPosition.TopLeft;
            plotModel.LegendBackground = OxyColor.FromAColor(200, OxyColors.White);

            LinearAxis offsetAxis = new LinearAxis() {
                MajorGridlineStyle = LineStyle.Dot,
                MinorGridlineStyle = LineStyle.None,
                Position = AxisPosition.Bottom,
                Title = "Offset minutes",
                AbsoluteMinimum = 0,
                MinorTickSize = 0
            };
            plotModel.Axes.Add(offsetAxis);
            LinearAxis valueAxis1 = new LinearAxis() {
                MajorGridlineStyle = LineStyle.Dot,
                MinorGridlineStyle = LineStyle.None,
                Position = AxisPosition.Left,
                StringFormat = "N0",
                IsZoomEnabled = false,
                IsPanEnabled = false,
                AbsoluteMinimum = 0,
                MaximumPadding = 0.2,
                MinorTickSize = 0
            };
            plotModel.Axes.Add(valueAxis1);

            plotModel.PlotMargins = new OxyThickness(70, 0, 0, 30);
            plotModel.Series.Clear();

            foreach (var ax in plotModel.Axes)
            {
                ax.AxisChanged += (sender, e) => SynchronizeCharts(plotModel, sender, e);
            }

            return plotModel;
        }

        private void SynchronizeCharts(PlotModel plotModel, object sender, AxisChangedEventArgs e)
        {
            if (DateTime.Now.Subtract(_lastAxisAdjust).TotalMilliseconds < 100)
            {
                return;
            }
            _lastAxisAdjust = DateTime.Now;

            try
            {

                double xstart = plotModel.DefaultXAxis.ActualMinimum;
                double xend = plotModel.DefaultXAxis.ActualMaximum;

                if (xstart < 0) xstart = 0;

                foreach (var pm in PlotModels)
                {
                    // set x zoom only for the charts not being zoomed
                    if (pm.Title != plotModel.Title)
                    {
                        pm.DefaultXAxis.Zoom(xstart, xend);
                    }
                    pm.InvalidatePlot(true);
                }

                InitializeQueries();

            }
            finally
            {
                _lastAxisAdjust = DateTime.Now;
            }
        }

        private Series LoadCpuSeries(WorkloadAnalysis analysis, OxyColor color)
        {
            if (analysis == null)
                return null;

            LineSeries cpuSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = false,
                TrackerFormatString = "Offset: {2:0}\n{0}: {4:0}",
                Title = analysis.Name,
                Color = color,
                Smooth = false
            };

            var Table = from t in analysis.Points
                        where ApplicationList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.ApplicationName)
                            && HostList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.HostName)
                            && DatabaseList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.DatabaseName)
                            && LoginList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.LoginName)
                        group t by new
                        {
                            offset = t.OffsetMinutes
                        }
                        into grp
                        orderby grp.Key.offset
                        select new
                        {
                            offset_minutes = grp.Key.offset,
                            cpu = grp.Sum(t => t.SumCpuUs)
                        };

            foreach (var p in Table)
            {
                cpuSeries.Points.Add(new DataPoint(p.offset_minutes , p.cpu));
            }

            return cpuSeries;
        }

        private Series LoadDurationSeries(WorkloadAnalysis analysis, OxyColor color)
        {
            if (analysis == null)
                return null;

            LineSeries durationSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = false,
                TrackerFormatString = "Offset: {2:0}\n{0}: {4:0}",
                Title = analysis.Name,
                Color = color, 
                Smooth = false
            };

            var Table = from t in analysis.Points
                        where ApplicationList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.ApplicationName)
                           && HostList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.HostName)
                           && DatabaseList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.DatabaseName)
                           && LoginList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.LoginName)
                        group t by new
                        {
                            offset = t.OffsetMinutes
                        }
                        into grp
                        orderby grp.Key.offset
                        select new
                        {
                            offset_minutes = grp.Key.offset,
                            duration = grp.Sum(t => t.SumDurationUs)
                        };

            foreach (var p in Table)
            {
                durationSeries.Points.Add(new DataPoint(p.offset_minutes, p.duration));
            }

            return durationSeries;
        }


        private Series LoadBatchesSeries(WorkloadAnalysis analysis, OxyColor color)
        {
            if (analysis == null)
                return null;

            LineSeries batchesSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = false,
                TrackerFormatString = "Offset: {2:0}\n{0}: {4:0}",
                Title = analysis.Name,
                Color = color,
                Smooth = false
            };


            var Table = from t in analysis.Points
                        where ApplicationList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.ApplicationName)
                           && HostList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.HostName)
                           && DatabaseList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.DatabaseName)
                           && LoginList.Where(f => f.IsChecked).Select(f => f.Name).Contains(t.LoginName)
                        group t by new
                        {
                            offset = t.OffsetMinutes
                        }
                        into grp
                        orderby grp.Key.offset
                        select new
                        {
                            offset_minutes = grp.Key.offset,
                            execution_count = grp.Sum(t => t.ExecutionCount / ((t.DurationMinutes == 0 ? 1 : t.DurationMinutes) * 60))
                        };

            foreach (var p in Table)
            {
                batchesSeries.Points.Add(new DataPoint(p.offset_minutes, p.execution_count));
            }

            return batchesSeries;
        }


        private void InitializeFilters()
        {
            var baseApplications = (
                from t in _baselineWorkloadAnalysis.Points
                group t by new { application = t.ApplicationName }
                into grp
                select grp.Key.application
            );
            if(_benchmarkWorkloadAnalysis != null)
            {
                baseApplications = baseApplications.Union(
                        from t in _benchmarkWorkloadAnalysis.Points
                        group t by new { application = t.ApplicationName }
                        into grp
                        select grp.Key.application
                    ).Distinct();
            }
            ApplicationList = new List<FilterDefinition>(
                    from name in baseApplications
                    orderby name
                    select new FilterDefinition() { Name = name, IsChecked = true }
                );

            var baseHosts = (
                from t in _baselineWorkloadAnalysis.Points
                group t by new { host = t.HostName }
                into grp
                select grp.Key.host
            );
            if (_benchmarkWorkloadAnalysis != null)
            {
                baseHosts = baseHosts.Union(
                        from t in _benchmarkWorkloadAnalysis.Points
                        group t by new { host = t.HostName }
                        into grp
                        select grp.Key.host
                    ).Distinct();
            }
            HostList = new List<FilterDefinition>(
                    from name in baseHosts
                    orderby name
                    select new FilterDefinition() { Name = name, IsChecked = true }
                );

            var baseDatabases = (
                from t in _baselineWorkloadAnalysis.Points
                group t by new { database = t.DatabaseName }
                into grp
                select grp.Key.database
            );
            if (_benchmarkWorkloadAnalysis != null)
            {
                baseDatabases = baseDatabases.Union(
                        from t in _benchmarkWorkloadAnalysis.Points
                        group t by new { database = t.DatabaseName }
                        into grp
                        select grp.Key.database
                    ).Distinct();
            }
            DatabaseList = new List<FilterDefinition>(
                    from name in baseDatabases
                    orderby name
                    select new FilterDefinition() { Name = name, IsChecked = true }
                );

            var baseLogins = (
                from t in _baselineWorkloadAnalysis.Points
                group t by new { login = t.LoginName }
                into grp
                select grp.Key.login
            );
            if (_benchmarkWorkloadAnalysis != null)
            {
                baseLogins = baseLogins.Union(
                        from t in _benchmarkWorkloadAnalysis.Points
                        group t by new { login = t.LoginName }
                        into grp
                        select grp.Key.login
                    ).Distinct();
            }
            LoginList = new List<FilterDefinition>(
                    from name in baseLogins
                    orderby name
                    select new FilterDefinition() { Name = name, IsChecked = true }
                );

            RaisePropertyChanged("ApplicationList");
            RaisePropertyChanged("HostList");
            RaisePropertyChanged("DatabaseList");
            RaisePropertyChanged("LoginList");
        }


        private void ShowStatusMessage(string message)
        {
            StatusMessage = message;
            RaisePropertyChanged("StatusMessage");
        }
            

    }
}
