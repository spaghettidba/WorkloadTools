using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
	[Serializable]
	public class ErrorWorkloadEvent : WorkloadEvent
	{
		public string Text { get; set; }
	}
}
