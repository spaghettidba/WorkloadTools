﻿using System;
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
            {
                return string.Empty;
            }

            IsPushedDown = true;
            var result = "";

            var hasPositives = false;
            var hasNegatives = false;

            for (var i = 0; i < ComparisonOperator.Length; i++)
            {
                if (ComparisonOperator[i] == FilterComparisonOperator.Not_Equal)
                {
                    hasNegatives = true;
                }
                else
                {
                    hasPositives = true;
                }
            }

            for (var i = 0; i < PredicateValue.Length; i++)
            {
                if (hasNegatives && hasPositives && ComparisonOperator[i] == FilterComparisonOperator.Not_Equal)
                {
                    // In this case I only care for the positives
                    continue;
                }

                var logicalOperator = "0"; // AND
                if (i > 0)
                {
                    if (hasNegatives && !hasPositives)
                    {
                        logicalOperator = " 0 "; // AND
                    }
                    else
                    {
                        result += " 1 "; // OR
                    }
                }

                result += "exec sp_trace_setfilter @TraceID, " + (byte)ColumnName + " ,  " + logicalOperator +", " + (byte)ComparisonOperator[i] + ", N'" + EscapeFilter(PredicateValue[i]) + "'";
            }
            return result;
        }

    }
}
