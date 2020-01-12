using OxyPlot;
using OxyPlot.Axes;
using OxyPlot.Series;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace WorkloadViewer.Model
{
    public class QueryDetails
    {
        public NormalizedQuery Query { get; private set; }
        private WorkloadAnalysis Benchmark { get; set; }
        private WorkloadAnalysis Baseline { get; set; }

        public QueryDetails(NormalizedQuery query, WorkloadAnalysis baseline, WorkloadAnalysis benchmark)
        {
            Query = query;
            Baseline = baseline;
            Benchmark = benchmark;
        }

        public DataTable QueryStats
        {
            get
            {
                return LoadQueryStats();
            }
        }

        public PlotModel DetailPlotModel
        {
            get
            {
                return LoadPlotModel();
            }
        }



        private DataTable LoadQueryStats()
        {
            DataTable result = new DataTable();
            result.Columns.Add(new DataColumn("Application", typeof(String)));
            result.Columns.Add(new DataColumn("Database", typeof(String)));
            result.Columns.Add(new DataColumn("Host", typeof(String)));
            result.Columns.Add(new DataColumn("Login", typeof(String)));
            result.Columns.Add(new DataColumn("avg_duration_us", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_duration_us2", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_cpu_us", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_cpu_us2", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_reads", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_reads2", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_writes", typeof(Int64)));
            result.Columns.Add(new DataColumn("avg_writes2", typeof(Int64)));
            result.Columns.Add(new DataColumn("execution_count", typeof(Int64)));
            result.Columns.Add(new DataColumn("execution_count2", typeof(Int64)));

            var baseline = from t in Baseline.Points
                           where t.NormalizedQuery.Hash == Query.Hash
                           group t by new
                           {
                               t.ApplicationName,
                               t.DatabaseName,
                               t.HostName,
                               t.LoginName
                           }
                           into grp
                           select new
                           {
                               grp.Key.ApplicationName,
                               grp.Key.DatabaseName,
                               grp.Key.HostName,
                               grp.Key.LoginName,
                               avg_duration_us = grp.Average(t => t.AvgDurationUs),
                               avg_cpu_us = grp.Average(t => t.AvgCpuUs),
                               avg_reads = grp.Average(t => t.AvgReads),
                               avg_writes = grp.Average(t => t.AvgWrites),
                               execution_count = grp.Sum(t => t.ExecutionCount)
                           };

            var benchmark = from t in baseline where false select new { t.ApplicationName, t.DatabaseName, t.HostName, t.LoginName, t.avg_duration_us, t.avg_cpu_us, t.avg_reads, t.avg_writes, t.execution_count };

            if (Benchmark != null)
            {
                benchmark = from t in Benchmark.Points
                            where t.NormalizedQuery.Hash == Query.Hash
                            group t by new
                            {
                                t.ApplicationName,
                                t.DatabaseName,
                                t.HostName,
                                t.LoginName
                            }
                            into grp
                            select new
                            {
                                grp.Key.ApplicationName,
                                grp.Key.DatabaseName,
                                grp.Key.HostName,
                                grp.Key.LoginName,
                                avg_duration_us = grp.Average(t => t.AvgDurationUs),
                                avg_cpu_us = grp.Average(t => t.AvgCpuUs),
                                avg_reads = grp.Average(t => t.AvgReads),
                                avg_writes = grp.Average(t => t.AvgWrites),
                                execution_count = grp.Sum(t => t.ExecutionCount)
                            };
            }

            foreach (var itm in baseline)
            {
                var newRow = result.Rows.Add();
                newRow["Application"] = itm.ApplicationName;
                newRow["Database"] = itm.DatabaseName;
                newRow["Host"] = itm.HostName;
                newRow["Login"] = itm.LoginName;
                newRow["avg_duration_us"] = itm.avg_duration_us;
                newRow["avg_cpu_us"] = itm.avg_cpu_us;
                newRow["avg_reads"] = itm.avg_reads;
                newRow["avg_writes"] = itm.avg_reads;
                newRow["execution_count"] = itm.execution_count;

                if (Benchmark != null)
                {
                    var _itm = from t in benchmark
                               where t.ApplicationName == itm.ApplicationName
                                  && t.DatabaseName == itm.DatabaseName
                                  && t.HostName == itm.HostName
                                  && t.LoginName == itm.LoginName
                               select new { t.avg_cpu_us, t.avg_duration_us, t.avg_reads, t.avg_writes, t.execution_count };

                    var itm2 = _itm.ToList();

                    if(itm2.Count > 0)
                    {
                        newRow["avg_duration_us2"] = itm2[0].avg_duration_us;
                        newRow["avg_cpu_us2"] = itm2[0].avg_cpu_us;
                        newRow["avg_reads2"] = itm2[0].avg_reads;
                        newRow["avg_writes2"] = itm2[0].avg_reads;
                        newRow["execution_count2"] = itm2[0].execution_count;
                    }
                    else
                    {
                        newRow["avg_duration_us2"] = 0;
                        newRow["avg_cpu_us2"] = 0;
                        newRow["avg_reads2"] = 0;
                        newRow["avg_writes2"] = 0;
                        newRow["execution_count2"] = 0;
                    }
                }

            }

            foreach (var itm in benchmark)
            {
                var res = from row in result.AsEnumerable()
                          where row.Field<string>("Application") == itm.ApplicationName
                             && row.Field<string>("Database") == itm.DatabaseName
                             && row.Field<string>("Host") == itm.HostName
                             && row.Field<string>("Login") == itm.LoginName
                          select row;

                if (res.Count() == 0)
                {
                    var newRow = result.Rows.Add();
                    newRow["Application"] = itm.ApplicationName;
                    newRow["Database"] = itm.DatabaseName;
                    newRow["Host"] = itm.HostName;
                    newRow["Login"] = itm.LoginName;
                    newRow["avg_duration_us2"] = itm.avg_duration_us;
                    newRow["avg_cpu_us2"] = itm.avg_cpu_us;
                    newRow["avg_reads2"] = itm.avg_reads;
                    newRow["avg_writes2"] = itm.avg_reads;
                    newRow["execution_count2"] = itm.execution_count;

                    var _itm = from t in baseline
                                where t.ApplicationName == itm.ApplicationName
                                    && t.DatabaseName == itm.DatabaseName
                                    && t.HostName == itm.HostName
                                    && t.LoginName == itm.LoginName
                                select new { t.avg_cpu_us, t.avg_duration_us, t.avg_reads, t.avg_writes, t.execution_count };

                    var itm2 = _itm.ToList();

                    if (itm2.Count > 0)
                    {
                        newRow["avg_duration_us"] = itm2[0].avg_duration_us;
                        newRow["avg_cpu_us"] = itm2[0].avg_cpu_us;
                        newRow["avg_reads"] = itm2[0].avg_reads;
                        newRow["avg_writes"] = itm2[0].avg_reads;
                        newRow["execution_count"] = itm2[0].execution_count;
                    }
                    else
                    {
                        newRow["avg_duration_us"] = 0;
                        newRow["avg_cpu_us"] = 0;
                        newRow["avg_reads"] = 0;
                        newRow["avg_writes"] = 0;
                        newRow["execution_count"] = 0;
                    }
                    
                }
            }

            return result;
            
        }

        private PlotModel LoadPlotModel()
        {
            PlotModel plotModel = new PlotModel();
            plotModel.LegendOrientation = LegendOrientation.Horizontal;
            plotModel.LegendPlacement = LegendPlacement.Inside;
            plotModel.LegendPosition = LegendPosition.TopLeft;
            plotModel.LegendBackground = OxyColor.FromAColor(200, OxyColors.White);
            plotModel.Title = "Average Duration";

            LinearAxis offsetAxis = new LinearAxis()
            {
                MajorGridlineStyle = LineStyle.Dot,
                MinorGridlineStyle = LineStyle.None,
                Position = AxisPosition.Bottom,
                Title = "Offset minutes",
                AbsoluteMinimum = 0,
                MinorTickSize = 0
            };
            plotModel.Axes.Add(offsetAxis);
            LinearAxis valueAxis1 = new LinearAxis()
            {
                MajorGridlineStyle = LineStyle.Dot,
                MinorGridlineStyle = LineStyle.None,
                Position = AxisPosition.Left,
                StringFormat = "N0",
                IsZoomEnabled = false,
                AbsoluteMinimum = 0,
                MaximumPadding = 0.2,
                MinorTickSize = 0,
                Title = "Duration (us)"
            };
            plotModel.Axes.Add(valueAxis1);

            plotModel.PlotMargins = new OxyThickness(70, 0, 0, 30);
            plotModel.Series.Clear();

            plotModel.Series.Add(LoadDurationSeries(Baseline, OxyColor.Parse("#01B8AA")));

            if(Benchmark != null)
            {
                plotModel.Series.Add(LoadDurationSeries(Benchmark, OxyColor.Parse("#000000")));
            }
            return plotModel;
        }


        private Series LoadDurationSeries(WorkloadAnalysis analysis, OxyColor color)
        {
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
                        where t.NormalizedQuery.Hash == Query.Hash
                        group t by new
                        {
                            offset = t.OffsetMinutes
                        }
                        into grp
                        orderby grp.Key.offset
                        select new
                        {
                            offset_minutes = grp.Key.offset,
                            duration = grp.Average(t => t.AvgDurationUs)
                        };

            foreach (var p in Table)
            {
                durationSeries.Points.Add(new DataPoint(p.offset_minutes, p.duration));
            }

            return durationSeries;
        }
    }
}
