using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NFX.ApplicationModel.Pile;

namespace WorkloadTools
{
    public class MMFEventQueue : IDisposable , IEventQueue
    {
        private ConcurrentQueue<PilePointer> pointers;
        private MMFPile pile;

        // this has no effect on a memory mapped file...
        public int BufferSize { get; set; }

        public MMFEventQueue()
        {
            pile = new MMFPile("workloadevents");
            pile.DataDirectoryRoot = System.IO.Path.GetTempPath();
            pointers = new ConcurrentQueue<PilePointer>();
            pile.Start();
        }


        public bool TryDequeue(out WorkloadEvent result)
        {
            try
            {
                PilePointer pp;
                if (!pointers.TryDequeue(out pp))
                {
                    result = null;
                    return false;
                }
                result = (WorkloadEvent)pile.Get(pp);
                pile.Delete(pp);
            }
            catch(Exception)
            {
                result = null;
                return false;
            }
            return true;
        }

        public void Enqueue(WorkloadEvent evt)
        {
            pointers.Enqueue(pile.Put(evt));
        }

        public void Dispose()
        {
            pile.WaitForCompleteStop();
            pile.Dispose();
        }
    }
}
