using System;

namespace WorkloadTools.Consumer.Analysis
{
    public class ExecutionDetailKey : IEquatable<ExecutionDetailKey>
    {
        public long Sql_hash { get; set; }
        public int Application_id { get; set; }
        public int Database_id { get; set; }
        public int Host_id { get; set; }
        public int Login_id { get; set; }

        public override int GetHashCode()
        {
            var hash = 497;
            unchecked
            {
                hash = (hash * 17) + Sql_hash.GetHashCode();
                hash = (hash * 17) + Application_id.GetHashCode();
                hash = (hash * 17) + Database_id.GetHashCode();
                hash = (hash * 17) + Host_id.GetHashCode();
                hash = (hash * 17) + Login_id.GetHashCode();
            }
            return hash;
        }

        public override bool Equals(object other)
        {
            return Equals(other as ExecutionDetailKey);
        }

        public bool Equals(ExecutionDetailKey other)
        {
            return other != null
                && Sql_hash.Equals(other.Sql_hash)
                && Application_id.Equals(other.Application_id)
                && Database_id.Equals(other.Database_id)
                && Host_id.Equals(other.Host_id)
                && Login_id.Equals(other.Login_id);
        }
    }
    
}

