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

namespace WorkloadViewer.ViewModel
{

    public class MainViewModel : ViewModelBase
    {

        private Options _options;

        public string StatusMessage { get; set; }
        public PlotModel CpuPlotModel { get; set; }
        public PlotModel DurationPlotModel { get; set; }
        public PlotModel BatchesPlotModel { get; set; }

        public List<FilterDefinition> HostList { get; set; }
        public List<FilterDefinition> ApplicationList { get; set; }
        public List<FilterDefinition> DatabaseList { get; set; }

        public ICommand LoadedCommand { get; set; }
        public ICommand RenderedCommand { get; set; }
        public ICommand KeyDownCommand { get; set; }


        private IDialogCoordinator _dialogCoordinator;
        private bool _IsAxisAdjusting = false;


        public MainViewModel()
        {
            LoadedCommand = new RelayCommand<EventArgs>(Loaded);
            RenderedCommand = new RelayCommand<EventArgs>(Rendered);
            KeyDownCommand = new RelayCommand<KeyEventArgs>(KeyDown);
            _dialogCoordinator = DialogCoordinator.Instance;
        }


        private void Rendered(EventArgs ev)
        {
            RefreshAllCharts();
        }


        private void Loaded(EventArgs ev)
        {
            ParseOptions();
            InitializeCharts();
            InitializeFilters();
        }

        private void ParseOptions()
        {
            _options = ((WorkloadViewer.App)App.Current).Options;

            if(_options.ConfigurationFile != null)
            {
                // read configuration from file
            }
            else
            {
                if(_options.BaselineServer == null || _options.BaselineDatabase == null)
                {
                    // display dialog
                }
            }

        }

        private void KeyDown(KeyEventArgs e)
        {
            if(e.Key == Key.F5)
            {
                // invoke refresh
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
            CpuPlotModel.Axes[1].Title = "Cpu (ms)";
            CpuPlotModel.Title = "Cpu";
            CpuPlotModel.Series.Add(LoadCpuSeries());
            //if compareMode then add second series

            DurationPlotModel = InitializePlotModel();
            DurationPlotModel.Axes[1].Title = "Duration (ms)";
            DurationPlotModel.Title = "Duration";
            DurationPlotModel.Series.Add(LoadDurationSeries());

            BatchesPlotModel = InitializePlotModel();
            BatchesPlotModel.Axes[1].Title = "Batches/second";
            BatchesPlotModel.Title = "Batches/second";
            BatchesPlotModel.Series.Add(LoadBatchesSeries());
        }


        private PlotModel InitializePlotModel()
        {
            PlotModel plotModel = new PlotModel();
            plotModel.LegendOrientation = LegendOrientation.Horizontal;
            plotModel.LegendPlacement = LegendPlacement.Inside;
            plotModel.LegendPosition = LegendPosition.TopLeft;
            plotModel.LegendBackground = OxyColor.FromAColor(200, OxyColors.White);

            DateTimeAxis dateAxis1 = new DateTimeAxis() {
                MajorGridlineStyle = LineStyle.Dot,
                MinorGridlineStyle = LineStyle.Dot,
                IntervalLength = 80,
                Position = AxisPosition.Bottom,
                Title = "Date",
                StringFormat = "dd/M/yy HH:mm"
            };
            plotModel.Axes.Add(dateAxis1);
            LinearAxis valueAxis1 = new LinearAxis() {
                MajorGridlineStyle = LineStyle.Dot, 
                MinorGridlineStyle = LineStyle.Dot, 
                Position = AxisPosition.Left
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
            if (_IsAxisAdjusting)
            {
                return;
            }
            _IsAxisAdjusting = true;

            try
            {

                double xstart = plotModel.DefaultXAxis.ActualMinimum;
                double xend = plotModel.DefaultXAxis.ActualMaximum;
                double ystart = plotModel.DefaultYAxis.ActualMinimum;
                double yend = plotModel.DefaultYAxis.ActualMaximum;

                foreach (var pm in (new List<PlotModel>() { CpuPlotModel, DurationPlotModel, BatchesPlotModel }).Where(p => p.Title != plotModel.Title))
                {
                    pm.DefaultXAxis.Zoom(xstart, xend);
                    pm.InvalidatePlot(true);
                }

            }
            finally
            {
                _IsAxisAdjusting = false;
            }
        }

        private Series LoadCpuSeries()
        {
            LineSeries cpuSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = true,
                TrackerFormatString = "Date: {2:yyyy-MM-dd HH:mm}\n{0}: {4:0}",
                Title = "Baseline",
                Smooth = true
            };

            for(int i=0; i<100; i++)
            {
                cpuSeries.Points.Add(new DataPoint(DateTimeAxis.ToDouble(DateTime.Now.AddMinutes(-i)), 100));
            }

            return cpuSeries;
        }

        private Series LoadDurationSeries()
        {
            LineSeries cpuSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = true,
                TrackerFormatString = "Date: {2:yyyy-MM-dd HH:mm}\n{0}: {4:0}",
                Title = "Baseline",
                Smooth = true
            };

            for (int i = 0; i < 100; i++)
            {
                cpuSeries.Points.Add(new DataPoint(DateTimeAxis.ToDouble(DateTime.Now.AddMinutes(-i)), 100));
            }

            return cpuSeries;
        }


        private Series LoadBatchesSeries()
        {
            LineSeries cpuSeries = new LineSeries()
            {
                StrokeThickness = 2,
                MarkerSize = 3,
                MarkerStroke = OxyColor.Parse("#FF0000"), //Red
                MarkerType = MarkerType.None,
                CanTrackerInterpolatePoints = true,
                TrackerFormatString = "Date: {2:yyyy-MM-dd HH:mm}\n{0}: {4:0}",
                Title = "Baseline",
                Smooth = true
            };

            for (int i = 0; i < 100; i++)
            {
                cpuSeries.Points.Add(new DataPoint(DateTimeAxis.ToDouble(DateTime.Now.AddMinutes(-i)), 100));
            }

            return cpuSeries;
        }


        private void InitializeFilters()
        {
            ApplicationList = new List<FilterDefinition>();
            ApplicationList.Add(new FilterDefinition() { Name = "Test", IsChecked = true });
            RaisePropertyChanged("ApplicationList");
        }

    }
}