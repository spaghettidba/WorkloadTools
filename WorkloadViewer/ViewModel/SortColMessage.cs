using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.ViewModel
{
    public class SortColMessage
    {
        public string ColumnName { get; set; }
        public ListSortDirection Direction { get; set;  }

        public SortColMessage(string columnName, ListSortDirection direction)
        {
            ColumnName = columnName;
            Direction = direction;
        }
    }
}
