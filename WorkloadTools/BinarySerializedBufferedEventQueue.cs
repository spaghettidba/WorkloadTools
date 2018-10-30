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

        private static string FILE_NAME_UNIQUIFIER = DateTime.Now.ToString("yyyyMMddHHmm") + "_" 
            + ("000000000" + System.Diagnostics.Process.GetCurrentProcess().Id).Right(9) + "_";

        private BinaryFormatter _formatter = new BinaryFormatter();

        public BinarySerializedBufferedEventQueue() : base()
        {
            baseFolder = Path.Combine(Path.Combine(System.IO.Path.GetTempPath(), "WorkloadTools"), "SerializedEventQueue");
            System.IO.Directory.CreateDirectory(baseFolder);
            _minFile = 0;
            _maxFile = 0;
        }

        protected override WorkloadEvent[] ReadEvents(int count)
        {
            WorkloadEvent[] result = null;
            string destFile = Path.Combine(baseFolder, FILE_NAME_UNIQUIFIER + ("000000000" + _minFile).Right(9) + ".cache");
            
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
            string destFile = Path.Combine(baseFolder, FILE_NAME_UNIQUIFIER);
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
                string destFile = Path.Combine(baseFolder, FILE_NAME_UNIQUIFIER);
                destFile += ("000000000" + i).Right(9) + ".cache";
                if (File.Exists(destFile))
                {
                    File.Delete(destFile);
                }

            }
        }


        public static void Test()
        {

            using (BinarySerializedBufferedEventQueue queue = new BinarySerializedBufferedEventQueue())
            {
                queue.BufferSize = 10000;
                Random r = new Random();
                Stopwatch watch = new Stopwatch();

                for (int j = 0; j < 40; j++)
                {
                    watch.Reset();
                    watch.Start();

                    
                    int numElements = (int)(r.NextDouble() * 100000);

                    for (int i = 0; i < numElements; i++)
                    {
                        queue.Enqueue(new ExecutionWorkloadEvent()
                        {
                            Text = $"SELECT {i} FROM sometable WHERE somecolumn = someValue ORDER BY someOtherColumn"
                        });
                    }

                    watch.Stop();
                    Console.WriteLine($"Enqueue {numElements} elements elapsed: {watch.Elapsed}");


                    int queueLen = queue.Count;

                    numElements = (int)(r.NextDouble() * 100000);

                    while (numElements > queueLen)
                        numElements -= 500;

                    watch.Reset();
                    watch.Start();

                    for (int i = 0; i < numElements; i++)
                    {
                        WorkloadEvent evt = null;
                        queue.TryDequeue(out evt);
                        //Console.WriteLine(((ExecutionWorkloadEvent)evt).Text);
                    }

                    watch.Stop();
                    Console.WriteLine($"Dequeue {numElements} elements elapsed: {watch.Elapsed}");
                }

                watch.Reset();
                watch.Start();

                int len = queue.Count;
                WorkloadEvent evnt = null;
                while (queue.TryDequeue(out evnt)) ;

                watch.Stop();
                Console.WriteLine($"Dequeue all {len} remaining elements elapsed: {watch.Elapsed}");
            }

            Environment.Exit(0);
        }


        public static void Test2()
        {
            int[] numbers = {
                  +9772
                , -9479
                , +70255
                , -49216
                , +18796
                , -39641
                , +60528
                , -60690
                , +78808
                , -49406
                , +42422
                , -72132
                , +65861
                , -34935
                , +55297
                , -10699
                , +96237
                , -72432
                , +55697
                , -85962
                , +18370
                , -72056
                , +97085
                , -50146
                , +43353
                , -53808
                , +28408
                , -76107
                , +51235
                , -50290
                , +67421
                , -9696
                , +65303
                , -45014
                , +53121
                , -50691
                , +68663
                , -54973
                , +34989
                , -66099
                , +15014
                , -53872
                , +97248
                , -38096
                , +705
                , -23998
                , +13872
                , -42048
                , +77390
                , -71767
                , +86413
                , -6260
                , +61030
                , -51330
                , +14412
                , -37716
                , +16394
                , -20109
                , +5862
                , -64988
                , +67733
                , -84421
                , +23954
                , -3518
                , +81985
                , -32726
                , +14828
                , -20847
                , +81813
                , -4605
                , +42036
                , -41263
                , +37442
                , -89598
                , +70947
                , -64497
                , +74808
                , -58988
                , +49441
                , -19355
                , -166474 };

            using (BinarySerializedBufferedEventQueue queue = new BinarySerializedBufferedEventQueue())
            {
                queue.BufferSize = 10000;
                int total = 0;
                for (int i = 0; i < numbers.Length; i++)
                {
                    if(i == 28)
                    {
                        Debug.WriteLine("Uh oh");
                    }
                    int num = numbers[i];
                    if(num > 0)
                    {
                        int initialCount = queue.Count;
                        for (int j = 0; j < num; j++)
                        {
                            queue.Enqueue(new ExecutionWorkloadEvent() { Text = $"SELECT {j} FROM sometable WHERE somecolumn = someValue ORDER BY someOtherColumn" });
                            if(i == 28)
                            {
                                Console.WriteLine($" {j}: should be {initialCount + j +1}  | is {queue.Count}");
                                if( initialCount + j + 1 == 8854)
                                {
                                    Console.WriteLine($"Aaaaah!");
                                }
                            }
                        }
                    }
                    else
                    {
                        int initialCount = queue.Count;
                        num = num * -1;
                        WorkloadEvent evnt = null;
                        for (int k = 0; k < num; k++)
                        {
                            queue.TryDequeue(out evnt);
                            //Console.WriteLine($" {k}: should be {initialCount - k}  | is {queue.Count}");
                        }
                        num = num * -1;
                    }
                    total += num;
                    Debug.Assert(queue.Count == total);
                }
            }

            Environment.Exit(0);
        }
    }
}
