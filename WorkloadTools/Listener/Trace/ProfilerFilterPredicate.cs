namespace WorkloadTools.Listener.Trace
{
    public class ProfilerFilterPredicate : FilterPredicate
    {
        public ProfilerFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public override string PushDown()
        {
            IsPushedDown = false;
            return string.Empty;
        }
    }
}
