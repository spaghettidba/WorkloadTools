using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    class TraceFilterPredicate : FilterPredicate
    {
        public TraceFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public override string PushDown()
        {
            if (!IsPredicateSet)
                return String.Empty;

            IsPushedDown = true;
            return "exec sp_trace_setfilter @TraceID, " + (byte)ColumnName + " ,  0, 0, N'" + EscapeFilter(EqualityPredicate) + "'"; 
        }

    }
}
