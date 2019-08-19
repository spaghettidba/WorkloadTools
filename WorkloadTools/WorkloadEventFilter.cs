using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class WorkloadEventFilter
    {
        public FilterPredicate ApplicationFilter { get; set; }
        public FilterPredicate DatabaseFilter { get; set; }
        public FilterPredicate HostFilter { get; set; }
        public FilterPredicate LoginFilter { get; set; }


        public WorkloadEventFilter()
        {
        }

        public bool Evaluate(WorkloadEvent evnt)
        {
            // don't filter events that are not supposed to be filtered
            if (!(evnt is ExecutionWorkloadEvent))
                return true;

            ExecutionWorkloadEvent evt = (ExecutionWorkloadEvent)evnt;

            if (evt.Type != WorkloadEvent.EventType.BatchCompleted && evt.Type != WorkloadEvent.EventType.RPCCompleted)
                return false;

            if (!(DatabaseFilter.IsPredicateSet || LoginFilter.IsPredicateSet || HostFilter.IsPredicateSet || ApplicationFilter.IsPredicateSet))
                return true;

            bool applicationFilterResults = !ApplicationFilter.IsPredicateSet || ApplicationFilter.IsPushedDown;
            bool databaseFilterResults = !DatabaseFilter.IsPredicateSet || DatabaseFilter.IsPushedDown;
            bool loginFilterResults = !LoginFilter.IsPredicateSet || LoginFilter.IsPushedDown;
            bool hostFilterResults = !HostFilter.IsPredicateSet || HostFilter.IsPushedDown;

            if (ApplicationFilter.IsPredicateSet && !ApplicationFilter.IsPushedDown)
            {
                applicationFilterResults = ApplicationFilter.PredicateValue.Contains(evt.ApplicationName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (DatabaseFilter.IsPredicateSet && !DatabaseFilter.IsPushedDown)
            {
                databaseFilterResults = DatabaseFilter.PredicateValue.Contains(evt.DatabaseName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (LoginFilter.IsPredicateSet && !LoginFilter.IsPushedDown)
            {
                loginFilterResults = LoginFilter.PredicateValue.Contains(evt.LoginName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (HostFilter.IsPredicateSet && !HostFilter.IsPushedDown)
            {
                hostFilterResults = HostFilter.PredicateValue.Contains(evt.HostName, StringComparer.CurrentCultureIgnoreCase);
            }

            return applicationFilterResults && databaseFilterResults && loginFilterResults && hostFilterResults;
        }

        public void PushDown(FilterPredicate predicate)
        {
            predicate.PushDown();
        }

    }
}
