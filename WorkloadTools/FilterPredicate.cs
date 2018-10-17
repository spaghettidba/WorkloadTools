using System;

namespace WorkloadTools
{
    public abstract class FilterPredicate
    {
        private string _predicateValue;

        public enum FilterColumnName : byte
        {
            DatabaseName = 35,
            HostName = 8,
            ApplicationName = 10,
            LoginName = 11
        }

        public enum FilterComparisonOperator : byte
        {
            Equal = 0,
            Not_Equal = 1,
            Greater_Than = 2,
            Less_Than = 3,
            Greater_Than_Or_Equal = 4,
            Less_Than_Or_Equal = 5,
            LIKE = 6,
            NOT_LIKE = 7
        }

        public FilterPredicate()
        {
        }


        public FilterColumnName ColumnName { get; set; }
        public string PredicateValue {
            get => _predicateValue;
            set {
                if (!String.IsNullOrEmpty(value) && value.StartsWith("^"))
                {
                    _predicateValue = value.Substring(1);
                    ComparisonOperator = FilterComparisonOperator.Not_Equal;
                }
                else
                {
                    _predicateValue = value;
                }
            }
        }
        public FilterComparisonOperator ComparisonOperator { get; set; } = FilterComparisonOperator.Equal;
        public bool IsPredicateSet { get { return !String.IsNullOrEmpty(PredicateValue); } }
        public bool IsPushedDown { get; set; } = false;

        public FilterPredicate(FilterColumnName name)
        {
            ColumnName = name;
        }

        public abstract string PushDown();

        protected string EscapeFilter(string value)
        {
            return value.Replace("'", "''");
        }

        public static string ComparisonOperatorAsString(FilterComparisonOperator op)
        {
            string result = String.Empty;
            switch (op)
            {
                case FilterComparisonOperator.Equal:
                    result = "=";
                    break;
                case FilterComparisonOperator.Not_Equal:
                    result = "<>";
                    break;
                case FilterComparisonOperator.Greater_Than:
                    result = ">";
                    break;
                case FilterComparisonOperator.Less_Than:
                    result = "<";
                    break;
                case FilterComparisonOperator.Greater_Than_Or_Equal:
                    result = ">=";
                    break;
                case FilterComparisonOperator.Less_Than_Or_Equal:
                    result = "<=";
                    break;
                case FilterComparisonOperator.LIKE:
                    result = "LIKE";
                    break;
                case FilterComparisonOperator.NOT_LIKE:
                    result = "NOT LIKE";
                    break;
            }
            return result;
        }
    }
}
