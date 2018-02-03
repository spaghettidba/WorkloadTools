using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class WorkloadEventFilter
    {
        public string ApplicationFilter { get; set; }
        public bool HasApplicationFilter { get { return !String.IsNullOrEmpty(ApplicationFilter); } }
        public string DatabaseFilter { get; set; }
        public bool HasDatabaseFilter { get { return !String.IsNullOrEmpty(DatabaseFilter); } }
        public string HostFilter { get; set; }
        public bool HasHostFilter { get { return !String.IsNullOrEmpty(HostFilter); } }
        public string LoginFilter { get; set; }
        public bool HasLoginFilter { get { return !String.IsNullOrEmpty(LoginFilter); } }



        public bool Evaluate(WorkloadEvent evt)
        {
            if (evt.Type != WorkloadEvent.EventType.BatchCompleted || evt.Type != WorkloadEvent.EventType.RPCCompleted)
                return false;

            if (!(HasDatabaseFilter || HasLoginFilter || HasHostFilter || HasApplicationFilter))
                return true;

            bool applicationFilterResults = !HasApplicationFilter;
            bool databaseFilterResults = !HasDatabaseFilter;
            bool loginFilterResults = !HasLoginFilter;
            bool hostFilterResults = !HasHostFilter;

            if (HasApplicationFilter)
            {
                applicationFilterResults = ApplicationFilter.Split(',').Contains(evt.ApplicationName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (HasDatabaseFilter)
            {
                databaseFilterResults = DatabaseFilter.Split(',').Contains(evt.DatabaseName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (HasLoginFilter)
            {
                loginFilterResults = LoginFilter.Split(',').Contains(evt.LoginName, StringComparer.CurrentCultureIgnoreCase);
            }

            if (HasHostFilter)
            {
                hostFilterResults = HostFilter.Split(',').Contains(evt.HostName, StringComparer.CurrentCultureIgnoreCase);
            }

            return applicationFilterResults && databaseFilterResults && loginFilterResults && hostFilterResults;
        }
    }
}
