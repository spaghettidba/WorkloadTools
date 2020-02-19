using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools.Listener
{
    // This class is used internally to keep track
    // of the files, offsets and event_sequences
    // when reading events from extended events or trace files
    internal class ReadIteration
    {
        public const int DEFAULT_TRACE_INTERVAL_SECONDS = 5;
        public const int DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD = 500;
        public const long TRACE_DEFAULT_OFFSET = 500;

        #region staticStuff
        private static int _lastFileHash;
        private static long _lastOffset;

        private static Dictionary<string, SortedSet<long>> recordedOffsets = new Dictionary<string, SortedSet<long>>();

        private static void AddOffset(string filename, long offset)
        {
            // perf optimization: most of the time the last 
            // file/offset pair is passed over and over again
            if (filename.GetHashCode() == _lastFileHash && offset == _lastOffset)
            {
                return;
            }

            // new values? ok, let's add them
            // one more check doesn't hurt though
            SortedSet<long> offsets = null;
            if (recordedOffsets.TryGetValue(filename, out offsets))
            {
                if (!offsets.Contains(offset))
                {
                    offsets.Add(offset);
                }
            }
            else
            {
                offsets = new SortedSet<long>();
                offsets.Add(offset);
                recordedOffsets.Add(filename, offsets);
            }

            // let's keep track of the last inserted values
            _lastFileHash = filename.GetHashCode();
            _lastOffset = offset;
        }

        public static long GetLastOffset(string filename)
        {
            long result = -1;
            SortedSet<long> offsets = null;
            if (recordedOffsets.TryGetValue(filename, out offsets))
            {
                result = offsets.Max();
            }
            return result;
        }

        public static long GetSecondLastOffset(string filename)
        {
            long result = -1;
            SortedSet<long> offsets = null;
            if (recordedOffsets.TryGetValue(filename, out offsets))
            {
                if (offsets.Count >= 2)
                {
                    result = offsets.ElementAt(offsets.Count - 2);
                }
            }
            return result;
        }
        #endregion staticStuff

        public string StartFileName { get; set; }
        public string EndFileName { get; set; }
        public long MinOffset { get; set; }
        public long StartOffset { get; set; }
        private long _endOffset = -1;
        public long EndOffset
        {
            get
            {
                return _endOffset;
            }
            set
            {
                // add current offset
                AddOffset(EndFileName, value);

                // set new value
                _endOffset = value;
            }
        }
        public long StartSequence { get; set; }
        public long EndSequence { get; set; }
        public long RowsRead { get; set; }
        public int Files { get; set; }



        // try to identify the root part of the rollover file name
        // the root is the part of the name before the numeric suffix
        // EG: mySessionName1234.xel => root = mySessionName
        public string GetXEFilePattern()
        {
            string filePattern = "";
            for (int j = StartFileName.Length - 4; j > 1 && StartFileName.Substring(j - 1, 1).All(char.IsDigit); j--)
            {
                filePattern = StartFileName.Substring(0, j - 1);
            }
            filePattern += "*.xel";
            return filePattern;
        }


        // Initial offset to be used as a parameter to the fn_xe_file_target_read_file function
        public long GetInitialOffset()
        {
            long result = -1;
            if (MinOffset > result)
                result = MinOffset;
            if (StartOffset > result)
                result = StartOffset;
            if (EndOffset > result)
                result = EndOffset;
            return result;
        }

        public long GetInitialSequence()
        {
            long result = -1;
            if (StartSequence > result)
                result = StartSequence;
            if (EndSequence > result)
                result = EndSequence;
            return result;
        }
    }
}
