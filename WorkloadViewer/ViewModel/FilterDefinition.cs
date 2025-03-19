using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.ViewModel
{
    public class FilterDefinition : IComparable, IEquatable<FilterDefinition>
    {
        public string Name { get; set; }
        public bool IsChecked { get; set; }

        public int CompareTo(object obj)
        {
            var result = -1;
            if(obj is FilterDefinition)
            {
                result = Name.CompareTo(((FilterDefinition)obj).Name);
            }
            return result;
        }

        public bool Equals(FilterDefinition other)
        {
            return CompareTo(other) == 0;
        }
    }
}
