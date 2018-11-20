using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.Model
{
    public class NormalizedQuery
    {
        public long Hash { get; set; }
        public string NormalizedText { get; set; }
        public string ExampleText { get; set; }
    }
}

