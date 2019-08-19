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

                result += ColumnName.ToString();
                result += " " + FilterPredicate.ComparisonOperatorAsString(ComparisonOperator[i]) + " '" + EscapeFilter(PredicateValue[i]) + "'";
            }
            result += ")";
            return result;
        }
    }
}