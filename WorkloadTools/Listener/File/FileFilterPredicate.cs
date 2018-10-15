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
            IsPushedDown = false;
            return String.Empty;
        }
    }
}