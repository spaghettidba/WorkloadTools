using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class FilterPredicate
    {
        public enum FilterColumnName: byte
        {
            DatabaseName = 35,
            HostName = 8,
            ApplicationName = 10,
            LoginName = 11
        }

        public FilterColumnName ColumnName { get; set; }
        public string EqualityPredicate { get; set; }
        public bool IsPredicateSet { get { return !String.IsNullOrEmpty(EqualityPredicate); } }
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
    }
}
