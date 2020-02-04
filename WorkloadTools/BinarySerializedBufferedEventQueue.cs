using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;
using WorkloadTools.Util;
using System.Diagnostics;

namespace WorkloadTools
{
    public class BinarySerializedBufferedEventQueue : BufferedEventQueue
    {
        private string baseFolder;

        private int _minFile, _maxFile;

        private string file_name_uniquifier = "";

        private BinaryFormatter _formatter = new BinaryFormatter();

        public BinarySerializedBufferedEventQueue() : base()
        {
            file_name_uniquifier = DateTime.Now.ToString("yyyyMMddHHmm") + "_" + ("000000000" + (Environment.TickCount & Int32.MaxValue)).Right(9) + "_";
            baseFolder = Path.Combine(Path.Combine(System.IO.Path.GetTempPath(), "WorkloadTools"), "SerializedEventQueue");
            System.IO.Directory.CreateDirectory(baseFolder);
            _minFile = 0;
            _maxFile = 0;

        }

        protected override WorkloadEvent[] ReadEvents(int count)
        {
            WorkloadEvent[] result = null;
            string destFile = Path.Combine(baseFolder, file_name_uniquifier + ("000000000" + _minFile).Right(9) + ".cache");
            
            using (System.IO.FileStream fileStream = new System.IO.FileStream(destFile, System.IO.FileMode.Open))
            using (BufferedStream bufferedStream = new BufferedStream(fileStream))
            {
                result = (WorkloadEvent[])_formatter.Deserialize(bufferedStream);
                if(result.Length != count)
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
            string destFile = Path.Combine(baseFolder, file_name_uniquifier);
            // c# does not have a String.Right method, so I created
            // an extension for it. Crazy, right?
            destFile += ("000000000" + _maxFile).Right(9) + ".cache";

            if (File.Exists(destFile))
            {
                File.Delete(destFile);
            }

            using (FileStream fileStream = new FileStream(destFile, FileMode.CreateNew))
            using (BufferedStream bufferedStream = new BufferedStream(fileStream))
            {
                _formatter.Serialize(bufferedStream, events);
                fileStream.Close();
            }
            _maxFile++;
        }

        protected override void Dispose(bool disposing)
        {
            // delete all pending files
            for (int i=_minFile; i<=_maxFile; i++)
            {
                string destFile = Path.Combine(baseFolder, file_name_uniquifier);
                destFile += ("000000000" + i).Right(9) + ".cache";
                if (File.Exists(destFile))
                {
                    File.Delete(destFile);
                }

            }
        }

    }
}
