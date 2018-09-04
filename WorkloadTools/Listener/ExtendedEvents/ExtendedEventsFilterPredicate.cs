using System;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class ExtendedEventsFilterPredicate : FilterPredicate
    {
        public ExtendedEventsFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public bool IsSqlAzure { get; set; }

        public override string PushDown()
        {
            if (!IsPredicateSet)
                return String.Empty;

            IsPushedDown = true;
            string result = "";

            switch (ColumnName)
            {
                case FilterColumnName.ApplicationName:
                    result = "sqlserver.client_app_name";
                    break;
                case FilterColumnName.HostName:
                    result = "sqlserver.client_hostname";
                    break;
                case FilterColumnName.LoginName:
                    if (IsSqlAzure)
                    {
                        result = "sqlserver.username";
                    }
                    else
                    {
                        result = "sqlserver.server_principal_name";
                    }
                    break;
                case FilterColumnName.DatabaseName:
                    result = "sqlserver.database_name";
                    break;
            }
            result += " " + FilterPredicate.ComparisonOperatorAsString(ComparisonOperator) + " N'" + EscapeFilter(PredicateValue) + "'";
            return result;
        }
    }
}