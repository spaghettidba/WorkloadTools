using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
    public abstract class BufferedEventQueue : IEventQueue 
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private int _bufferSize;

        public int BufferSize
        {
            get { return _bufferSize; }
            set { _bufferSize = value; initialize(); }
        }

        protected object syncRoot = new object();

        private WorkloadEvent[] _array;
        private WorkloadEvent[] _overflowArray;
        private int _head;               // First valid element in the queue
        private int _tail;               // Last valid element in the queue
        private int _size;               // Number of elements in the array
        private int _totOverflowSize;    // Total number of elements in the overflow array AND disk
        private int _overflowSize;       // Current number of elements in the overflow array
        private int _overflowBufferSize;

        public virtual int Count
        {
            get
            {
                lock (syncRoot)
                {
                    return _size + _totOverflowSize;
                }
            }
        }

        public BufferedEventQueue()
        {
            initialize();
        }

        private void initialize()
        {
            _array = new WorkloadEvent[BufferSize];
            _overflowArray = null;
            _overflowBufferSize = BufferSize / 2;
        }

        protected abstract void WriteEvents(WorkloadEvent[] events);

        protected abstract WorkloadEvent[] ReadEvents(int count);



        public virtual void Enqueue(WorkloadEvent evt)
        {
            lock (syncRoot)
            {
                if (_overflowArray != null)
                {
                    // write to the overflow array
                    _overflowArray[_overflowSize] = evt;
                    _overflowSize++;
                    _totOverflowSize++;
                    if(_overflowSize == _overflowBufferSize)
                    {
                        // decide what to do with the overflow:
                        // if we have enough room in the base array
                        // AND no events persisted to disk, then
                        // write events back to the queue
                        // if the base array does not have enough room, 
                        // write the overflow array to the database
                        // and allocate a new overflow array

                        if((_size <= BufferSize - _overflowBufferSize) && (_totOverflowSize - _overflowSize <= 0))
                        {
                            EnqueueAll(_overflowArray);
                            _overflowSize = 0;
                            _overflowArray = null;
                        }
                        else
                        {
                            WriteEvents(_overflowArray);
                            _overflowSize = 0;
                            _overflowArray = new WorkloadEvent[_overflowBufferSize];
                        }
                    }
                }
                else
                {
                    _array[_tail] = evt;
                    _tail = (_tail + 1) % _array.Length;
                    _size++;

                    // the internal array is at capacity: allocate an overflow array
                    // with size = 50% of BufferSize
                    if (_size == _array.Length)
                    {
                        _overflowArray = new WorkloadEvent[_overflowBufferSize];
                    }
                }
            }
        }


        private void EnqueueAll(WorkloadEvent[] source)
        {
            EnqueueAll(source, source.Length);
        }

        private void EnqueueAll(WorkloadEvent[] source, int count)
        {
            if(count > source.Length)
            {
                throw new ArgumentOutOfRangeException($"The 'count' argument ({count}) is greater than the length of the array to enqueue ({source.Length}).");
            }

            if (_head < _tail)
            {
                int numFirst = _array.Length - _tail;
                if (numFirst > count) numFirst = count;
                Array.Copy(source, 0, _array, _tail, numFirst);
                _tail = (_tail + numFirst) % _array.Length;
                if (numFirst < count)
                {
                    int numSecond = count - numFirst;
                    Array.Copy(source, numFirst, _array, _tail, numSecond);
                    _tail = (_tail + numSecond) % _array.Length;
                }
            }
            else
            {
                Array.Copy(source, 0, _array, _tail, count);
                _tail = (_tail + count) % _array.Length;
            }
            _size += count;
            _totOverflowSize -= count;
        }




        public virtual bool TryDequeue(out WorkloadEvent result)
        {
            result = null;
            try
            {
                lock (syncRoot)
                {
                    if (Count == 0)
                        return false;

                    result = _array[_head];
                    _array[_head] = null;
                    _head = (_head + 1) % _array.Length;
                    _size--;

                    if(_totOverflowSize > 0)
                    {

                        // if we have space available and overflowed 
                        // events on disk, then we read them back 
                        if (_totOverflowSize - _overflowSize > 0) 
                        {
                            if ((_size == _array.Length - _overflowBufferSize))
                            {
                                EnqueueAll(ReadEvents(_overflowBufferSize));
                            }
                        }
                        else
                        {
                            // if we have events in the overflow array (but not on disk)
                            // and enough space in the queue (at least 75% free), put them back
                            if (_overflowSize > 0 && _size <= _array.Length - (_overflowBufferSize + _overflowBufferSize / 2))
                            {
                                EnqueueAll(_overflowArray, _overflowSize);
                                _overflowSize = 0;
                                _overflowArray = null;
                                _totOverflowSize = 0;
                            }
                        }

                    }

                }
                return true;
            }
            catch (Exception e)
            {
                logger.Warn(e, "Unable to dequeue");
                result = null;
                return false;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);

        public bool HasMoreElements()
        {
            return Count > 0;
        }
    }
}
