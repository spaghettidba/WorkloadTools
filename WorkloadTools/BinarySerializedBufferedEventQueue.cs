using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using WorkloadTools.Util;
using System.Diagnostics;

namespace WorkloadTools
{
    public class BinarySerializedBufferedEventQueue : BufferedEventQueue
    {
        private readonly string baseFolder;

        private int _minFile, _maxFile;

        private readonly string file_name_uniquifier = "";

        public BinarySerializedBufferedEventQueue() : base()
        {
            file_name_uniquifier = DateTime.Now.ToString("yyyyMMddHHmm") + "_" + ("000000000" + (Environment.TickCount & int.MaxValue)).Right(9) + "_";
            baseFolder = Path.Combine(Path.Combine(System.IO.Path.GetTempPath(), "WorkloadTools"), "SerializedEventQueue");
            _ = System.IO.Directory.CreateDirectory(baseFolder);
            _minFile = 0;
            _maxFile = 0;
        }

        protected override WorkloadEvent[] ReadEvents(int count)
        {
            WorkloadEvent[] result = null;
            var destFile = Path.Combine(baseFolder, file_name_uniquifier + ("000000000" + _minFile).Right(9) + ".cache");

            using (var fileStream = new FileStream(destFile, FileMode.Open))
            using (var streamReader = new StreamReader(fileStream))
            {
                var json = streamReader.ReadToEnd();
                result = JsonConvert.DeserializeObject<WorkloadEvent[]>(json);
                if (result.Length != count)
                {
                    throw new ArgumentOutOfRangeException($"The deserialized array is of the wrong size (expected: {count}, found: {result.Length})");
                }
            }

            File.Delete(destFile);
            _minFile++;

            return result;
        }

        protected override void WriteEvents(WorkloadEvent[] events)
        {
            var destFile = Path.Combine(baseFolder, file_name_uniquifier);
            destFile += ("000000000" + _maxFile).Right(9) + ".cache";

            if (File.Exists(destFile))
            {
                File.Delete(destFile);
            }

            using (var fileStream = new FileStream(destFile, FileMode.CreateNew))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                var json = JsonConvert.SerializeObject(events);
                streamWriter.Write(json);
            }
            _maxFile++;
        }

        protected override void Dispose(bool disposing)
        {
            // delete all pending files
            for (var i = _minFile; i <= _maxFile; i++)
            {
                var destFile = Path.Combine(baseFolder, file_name_uniquifier);
                destFile += ("000000000" + i).Right(9) + ".cache";
                if (File.Exists(destFile))
                {
                    File.Delete(destFile);
                }
            }
        }
    }
}
