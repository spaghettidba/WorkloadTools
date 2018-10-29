namespace WorkloadTools
{
    public interface IEventQueue 
    {
        int BufferSize { get; set; }

        bool TryDequeue(out WorkloadEvent result);
        void Enqueue(WorkloadEvent evt);
    }
}