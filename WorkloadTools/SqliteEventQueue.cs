using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
    public class SqliteEventQueue : BufferedEventQueue
    {

        public SqliteEventQueue() : base()
        {
        }

        protected override void Dispose(bool disposing)
        {
            throw new NotImplementedException();
        }

        protected override WorkloadEvent[] ReadEvents(int count)
        {
            // STRATEGY:
            // do not attempt deleting rows returned
            // read all rows from the table and drop it
            throw new NotImplementedException();
        }

        protected override void WriteEvents(WorkloadEvent[] events)
        {
            // STRATEGY:
            // write to a table
            // the table name has an index postfix like cache01, cache02...
            throw new NotImplementedException();
        }
    }
}
