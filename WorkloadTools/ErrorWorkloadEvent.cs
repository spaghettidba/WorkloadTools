using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
	[Serializable]
	public class ErrorWorkloadEvent : ExecutionWorkloadEvent
	{
		public ErrorWorkloadEvent()
        {
            Type = EventType.Error;
        }
	}
}
