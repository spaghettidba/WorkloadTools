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
            string result = "(";
            for (int i = 0; i < PredicateValue.Length; i++)
            {
                if (i > 0) { result += " OR "; }
                result += ColumnName.ToString();
                result += " " + FilterPredicate.ComparisonOperatorAsString(ComparisonOperator) + " '" + EscapeFilter(PredicateValue[i]) + "'";
            }
            result += ")";
            return result;
        }
    }
}