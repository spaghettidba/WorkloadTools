using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class WaitStatsWorkloadEvent : WorkloadEvent
    {
        [ProtoIgnore]
        private DataTable _waits;

        [ProtoIgnore]
        public DataTable Waits
        {
            get
            {
                _waits ??= ToWaitsDataTable(WaitsList);
                return _waits;
            }

            set => _waits = value;
        }

        [ProtoMember(10)]
        public List<ColumnWaits> WaitsList { get; set; } = new();

        public WaitStatsWorkloadEvent()
        {
            Type = EventType.WAIT_stats;
        }

        [ProtoBeforeSerialization]
        private void BeforeSerialization()
        {
            if (_waits != null)
                WaitsList = ToWaitStats(_waits);
        }

        [ProtoAfterDeserialization]
        private void AfterDeserialization()
        {
            Waits = ToWaitsDataTable(WaitsList);
        }

        private static List<ColumnWaits> ToWaitStats(DataTable waits)
        {
            var list = new List<ColumnWaits>();

            if (waits == null)
                return list;

            foreach (DataRow row in waits.Rows)
            {
                list.Add(new ColumnWaits
                {
                    RowId = Convert.ToInt32(row["row_id"]),
                    WaitType = row["wait_type"].ToString(),
                    WaitSec = Convert.ToDouble(row["wait_sec"]),
                    ResourceSec = Convert.ToDouble(row["resource_sec"]),
                    SignalSec = Convert.ToDouble(row["signal_sec"]),
                    WaitCount = Convert.ToDouble(row["wait_count"])
                });
            }

            return list;
        }

        private static DataTable ToWaitsDataTable(List<ColumnWaits> waitsList)
        {
            var table = new DataTable();

            _ = table.Columns.Add("row_id", typeof(int));
            _ = table.Columns.Add("wait_type", typeof(string));
            _ = table.Columns.Add("wait_sec", typeof(double));
            _ = table.Columns.Add("resource_sec", typeof(double));
            _ = table.Columns.Add("signal_sec", typeof(double));
            _ = table.Columns.Add("wait_count", typeof(double));

            if (waitsList == null)
                return table;

            foreach (var wait in waitsList)
            {
                _ = table.Rows.Add(
                    wait.RowId,
                    wait.WaitType,
                    wait.WaitSec,
                    wait.ResourceSec,
                    wait.SignalSec,
                    wait.WaitCount);
            }

            return table;
        }
    }

    [ProtoContract]
    public class ColumnWaits
    {
        [ProtoMember(1)]
        public int RowId { get; set; }
        [ProtoMember(2)]
        public string WaitType { get; set; }
        [ProtoMember(3)]
        public double WaitSec { get; set; }
        [ProtoMember(4)]
        public double ResourceSec { get; set; }
        [ProtoMember(5)]
        public double SignalSec { get; set; }
        [ProtoMember(6)]
        public double WaitCount { get; set; }
    }
}
