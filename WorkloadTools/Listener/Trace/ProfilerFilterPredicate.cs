using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public class ProfilerFilterPredicate : FilterPredicate
    {
        public ProfilerFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public override string PushDown()
        {
            IsPushedDown = false;
            return String.Empty;
        }
    }
}
