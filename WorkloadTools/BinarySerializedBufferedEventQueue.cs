using System.Data;

using ProtoBuf;

using WorkloadTools.Util;

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
            
            using (var fileStream = new System.IO.FileStream(destFile, System.IO.FileMode.Open, FileAccess.Read))
            {
                result = ProtoBuf.Serializer.Deserialize<WorkloadEvent[]>(fileStream);
            }

            if(result.Length != count)
            {
                throw new ArgumentOutOfRangeException($"The deserialized array is of the wrong size (expected: {count}, found: {result.Length})");
            }

            File.Delete(destFile);
            _minFile++;

            return result;
        }

        protected override void WriteEvents(WorkloadEvent[] events)
        {
            var destFile = Path.Combine(baseFolder, file_name_uniquifier);
            // c# does not have a String.Right method, so I created
            // an extension for it. Crazy, right?
            destFile += ("000000000" + _maxFile).Right(9) + ".cache";

            if (File.Exists(destFile))
            {
                File.Delete(destFile);
            }

            using (var fileStream = new FileStream(destFile, FileMode.CreateNew, FileAccess.Write))
            {
                ProtoBuf.Serializer.Serialize(fileStream, events);
            }

            _maxFile++;
        }

        protected override void Dispose(bool disposing)
        {
            // delete all pending files
            for (var i=_minFile; i<=_maxFile; i++)
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
