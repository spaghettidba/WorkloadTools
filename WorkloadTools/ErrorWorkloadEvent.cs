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
