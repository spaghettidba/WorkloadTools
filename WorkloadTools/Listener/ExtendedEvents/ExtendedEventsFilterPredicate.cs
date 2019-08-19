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
            string result = "(";

            // Implementing multivalued filters with negative values
            // requires analyzing the syntax of the filters
            //
            // Let's say I have a filter like this:
            // "DatabaseFilter" = ["master","model","^tempdb","msdb"]
            //
            // It literally says I want master, model and msdb, but I don't want tempdb
            // In this case, it means that I want master, model and tempdb
            // But if I only had negative filters, it would mean anything but those databases.

            bool hasPositives = false;
            bool hasNegatives = false;

            for (int i = 0; i < ComparisonOperator.Length; i++)
            {
                if (ComparisonOperator[i] == FilterComparisonOperator.Not_Equal)
                    hasNegatives = true;
                else
                    hasPositives = true;
            }

            for (int i = 0; i < PredicateValue.Length; i++)
            {
                if (hasNegatives && hasPositives && ComparisonOperator[i] == FilterComparisonOperator.Not_Equal)
                {
                    // In this case I only care for the positives
                    continue;
                }

                if (i > 0)
                {
                    if (hasNegatives && !hasPositives) result += " AND ";
                    else result += " OR ";
                }

                switch (ColumnName)
                {
                    case FilterColumnName.ApplicationName:
                        result += "sqlserver.client_app_name";
                        break;
                    case FilterColumnName.HostName:
                        result += "sqlserver.client_hostname";
                        break;
                    case FilterColumnName.LoginName:
                        if (IsSqlAzure)
                        {
                            result += "sqlserver.username";
                        }
                        else
                        {
                            result += "sqlserver.server_principal_name";
                        }
                        break;
                    case FilterColumnName.DatabaseName:
                        result += "sqlserver.database_name";
                        break;
                }
                result += " " + FilterPredicate.ComparisonOperatorAsString(ComparisonOperator[i]) + " N'" + EscapeFilter(PredicateValue[i]) + "'";
            }
            result += ")";
            return result;
        }
    }
}