using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class DiskPerfWorkloadEvent : WorkloadEvent
    {
        [ProtoIgnore]
        private DataTable _diskPerf;

        [ProtoIgnore]
        public DataTable DiskPerf
        {
            get
            {
                _diskPerf ??= ToDiskPerfDataTable(DiskPerfList);
                return _diskPerf;
            }

            set => _diskPerf = value;
        }

        [ProtoMember(10)]
        public List<ColumnDiskPerf> DiskPerfList { get; set; } = new();

        public DiskPerfWorkloadEvent()
        {
            Type = EventType.DiskPerf;
        }

        [ProtoBeforeSerialization]
        private void BeforeSerialization()
        {
            if (_diskPerf != null)
                DiskPerfList = ToDiskPerfStats(_diskPerf);
        }

        [ProtoAfterDeserialization]
        private void AfterDeserialization()
        {
            DiskPerf = ToDiskPerfDataTable(DiskPerfList);
        }

        private static List<ColumnDiskPerf> ToDiskPerfStats(DataTable diskPerf)
        {
            var list = new List<ColumnDiskPerf>();

            if (diskPerf == null)
                return list;

            foreach (DataRow row in diskPerf.Rows)
            {
                list.Add(new ColumnDiskPerf
                {
                    RowId = Convert.ToInt32(row["row_id"]),
                    DatabaseName = row["database_name"].ToString(),
                    PhysicalFilename = row["physical_filename"].ToString(),
                    LocicalFilename = row["logical_filename"].ToString(),
                    FileType = row["file_type"].ToString(),
                    VolumeMountPoint = row["volume_mount_point"].ToString(),
                    ReadLatencyMs = Convert.ToDouble(row["read_latency_ms"]),
                    Reads = Convert.ToDouble(row["reads"]),
                    ReadBytes = Convert.ToDouble(row["read_bytes"]),
                    WriteLatencyMs = Convert.ToDouble(row["write_latency_ms"]),
                    Writes = Convert.ToDouble(row["writes"]),
                    WriteBytes = Convert.ToDouble(row["write_bytes"]),
                    CumReadLatencyMs = Convert.ToDouble(row["cum_read_latency_ms"]),
                    CumReads = Convert.ToDouble(row["cum_reads"]),
                    CumReadBytes = Convert.ToDouble(row["cum_read_bytes"]),
                    CumWritesLatencyMs = Convert.ToDouble(row["cum_write_latency_ms"]),
                    CumWrites = Convert.ToDouble(row["cum_writes"]),
                    CumWriteBytes = Convert.ToDouble(row["cum_write_bytes"])
                });
            }

            return list;
        }

        private static DataTable ToDiskPerfDataTable(List<ColumnDiskPerf> diskPerfList)
        {
            var table = new DataTable();

            _ = table.Columns.Add("row_id", typeof(int));
            _ = table.Columns.Add("database_name", typeof(string));
            _ = table.Columns.Add("physical_filename", typeof(string));
            _ = table.Columns.Add("logical_filename", typeof(string));
            _ = table.Columns.Add("file_type", typeof(string));
            _ = table.Columns.Add("volume_mount_point", typeof(string));
            _ = table.Columns.Add("read_latency_ms", typeof(double));
            _ = table.Columns.Add("reads", typeof(double));
            _ = table.Columns.Add("read_bytes", typeof(double));
            _ = table.Columns.Add("write_latency_ms", typeof(double));
            _ = table.Columns.Add("writes", typeof(double));
            _ = table.Columns.Add("write_bytes", typeof(double));
            _ = table.Columns.Add("cum_read_latency_ms", typeof(double));
            _ = table.Columns.Add("cum_reads", typeof(double));
            _ = table.Columns.Add("cum_read_bytes", typeof(double));
            _ = table.Columns.Add("cum_write_latency_ms", typeof(double));
            _ = table.Columns.Add("cum_writes", typeof(double));
            _ = table.Columns.Add("cum_write_bytes", typeof(double));

            if (diskPerfList == null)
                return table;

            foreach (var diskPerf in diskPerfList)
            {
                _ = table.Rows.Add(
                    diskPerf.RowId,
                    diskPerf.DatabaseName,
                    diskPerf.PhysicalFilename,
                    diskPerf.LocicalFilename,
                    diskPerf.FileType,
                    diskPerf.VolumeMountPoint,
                    diskPerf.ReadLatencyMs,
                    diskPerf.Reads,
                    diskPerf.ReadBytes,
                    diskPerf.WriteLatencyMs,
                    diskPerf.Writes,
                    diskPerf.WriteBytes,
                    diskPerf.CumReadLatencyMs,
                    diskPerf.CumReads,
                    diskPerf.CumReadBytes,
                    diskPerf.CumWritesLatencyMs,
                    diskPerf.CumWrites,
                    diskPerf.CumWriteBytes
                );
            }

            return table;
        }
    }

    [ProtoContract]
    public class ColumnDiskPerf
    {
        [ProtoMember(1)]
        public int RowId { get; set; }
        [ProtoMember(2)]
        public string? DatabaseName { get; set; }
        [ProtoMember(3)]
        public string? PhysicalFilename { get; set; }
        [ProtoMember(4)]
        public string? LocicalFilename { get; set; }
        [ProtoMember(5)]
        public string? FileType { get; set; }
        [ProtoMember(6)]
        public string? VolumeMountPoint { get; set; }
        [ProtoMember(7)]
        public double? ReadLatencyMs { get; set; }
        [ProtoMember(8)]
        public double? Reads { get; set; }
        [ProtoMember(9)]
        public double? ReadBytes { get; set; }
        [ProtoMember(10)]
        public double? WriteLatencyMs { get; set; }
        [ProtoMember(11)]
        public double? Writes { get; set; }
        [ProtoMember(12)]
        public double? WriteBytes { get; set; }
        [ProtoMember(13)]
        public double? CumReadLatencyMs { get; set; }
        [ProtoMember(14)]
        public double? CumReads { get; set; }
        [ProtoMember(15)]
        public double? CumReadBytes { get; set; }
        [ProtoMember(16)]
        public double? CumWritesLatencyMs { get; set; }
        [ProtoMember(17)]
        public double? CumWrites { get; set; }
        [ProtoMember(18)]
        public double? CumWriteBytes { get; set; }
    }

}

