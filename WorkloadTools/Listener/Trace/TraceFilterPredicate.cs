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
            string result = "";
            for (int i = 0; i < PredicateValue.Length; i++)
            {
                result += "exec sp_trace_setfilter @TraceID, " + (byte)ColumnName + " ,  " + (i == 0 ? 0:1) +", " + (byte)ComparisonOperator + ", N'" + EscapeFilter(PredicateValue[i]) + "'";
            }
            return result;
        }

    }
}
