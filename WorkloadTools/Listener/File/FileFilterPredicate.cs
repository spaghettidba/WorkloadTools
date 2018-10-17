using System;

namespace WorkloadTools.Listener.File
{
    public class FileFilterPredicate : FilterPredicate
    {
        public FileFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public bool IsSqlAzure { get; set; }

        public override string PushDown()
        {
            if (!IsPredicateSet)
                return String.Empty;

            IsPushedDown = true;
            string result = ColumnName.ToString();
            result += " " + FilterPredicate.ComparisonOperatorAsString(ComparisonOperator) + " N'" + EscapeFilter(PredicateValue) + "'";
            return result;
        }
    }
}