using System.Collections.Generic;

using WorkloadViewer.ViewModel;

public class QueryResultEqualityComparer : IEqualityComparer<QueryResult>
{
    public bool Equals(QueryResult x, QueryResult y)
    {
        return x?.query_hash == y?.query_hash;
    }

    public int GetHashCode(QueryResult obj)
    {
        return obj?.query_hash.GetHashCode() ?? 0;
    }
}