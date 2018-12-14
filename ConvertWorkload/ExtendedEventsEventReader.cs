using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Listener;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Util;

namespace ConvertWorkload
{
    namespace ConvertWorkload
    {
        public class ExtendedEventsEventReader : EventReader
        {
            private static Logger logger = LogManager.GetCurrentClassLogger();

            private string tracePath;
            private bool started = false;
            private bool finished = false;

            public ExtendedEventsEventReader(string path)
            {
                Events = new BinarySerializedBufferedEventQueue();
                Events.BufferSize = 10000;
                tracePath = path;
                Filter = new ExtendedEventsEventFilter();
            }


            private void ReadEventsFromFile()
            {
                try
                {
                    // get first trace rollover file
                    var parentDir = Directory.GetParent(tracePath);
                    var fileName = Path.GetFileNameWithoutExtension(tracePath) + "*" + Path.GetExtension(tracePath);

                    List<string> files = Directory.GetFiles(parentDir.FullName, fileName).ToList();
                    files.Sort();

                    SqlTransformer transformer = new SqlTransformer();

                    foreach (string xeFile in files)
                    {
                        ExtendedEventsFileReader reader = new ExtendedEventsFileReader(xeFile, Events);
                        reader.ReadEvents();
                        while (!reader.HasFinished)
                        {
                            Thread.Sleep(50);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message);

                    if (ex.InnerException != null)
                        logger.Error(ex.InnerException.Message);

                    Dispose();
                }
            }


            public override bool HasFinished()
            {
                return finished && !Events.HasMoreElements();
            }

            public override bool HasMoreElements()
            {
                return !finished && !stopped && (started ? Events.HasMoreElements() : true);
            }

            public override WorkloadEvent Read()
            {
                if (!started)
                {
                    Task t = Task.Factory.StartNew(ReadEventsFromFile);
                    started = true;
                }

                WorkloadEvent result = null;
                while (!Events.TryDequeue(out result))
                {
                    if (stopped)
                        return null;

                    Thread.Sleep(5);
                }
                return result;
            }

            protected override void Dispose(bool disposing)
            {
                if (!stopped)
                {
                    stopped = true;
                }
            }
        }
    }
}
