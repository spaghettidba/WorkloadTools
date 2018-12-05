using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.ViewModel
{
    public class Message
    {
        public string Text { get; set; }

        public Message(string name)
        {
            Text = name;
        }
    }
}
