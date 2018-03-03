namespace WorkloadTools.Listener.ExtendedEvents
{
    public class ExtendedEventsFilterPredicate : FilterPredicate
    {
        public ExtendedEventsFilterPredicate(FilterColumnName name) : base(name)
        {
        }

        public override string PushDown()
        {
            IsPushedDown = true;
            string result = "";
            switch (ColumnName)
            {
                case FilterColumnName.ApplicationName:
                    break;
                case FilterColumnName.HostName:
                    break;
                case FilterColumnName.LoginName:
                    break;
                case FilterColumnName.DatabaseName:
                    break;
            }
            return result;
        }
    }
}