using System;
using WorkloadTools;

namespace ConvertWorkload
{
    public abstract class EventReader : IDisposable
    {
        private WorkloadEventFilter _filter;

        public string[] ApplicationFilter { get; set; }
        public string[] DatabaseFilter { get; set; }
        public string[] HostFilter { get; set; }
        public string[] LoginFilter { get; set; }

        protected bool stopped;
        protected IEventQueue Events;

        protected WorkloadEventFilter Filter
        {
            get
            {
                if (_filter != null)
                {
                    _filter.ApplicationFilter.PredicateValue = ApplicationFilter;
                    _filter.DatabaseFilter.PredicateValue = DatabaseFilter;
                    _filter.HostFilter.PredicateValue = HostFilter;
                    _filter.LoginFilter.PredicateValue = LoginFilter;
                    return _filter;
                }
                else
                {
                    return null;
                }
            }
            set
            {
                _filter = value;
            }
        }

        public abstract bool HasFinished();

        public abstract WorkloadEvent Read();

        public abstract bool HasMoreElements();

        public void Dispose()
        {
            stopped = true;
            Events.Dispose();
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);
    }
}