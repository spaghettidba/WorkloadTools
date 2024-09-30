﻿using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer;

namespace WorkloadTools
{
    public class WorkloadController : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static string BaseLocation = new Uri(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().CodeBase)).LocalPath;

        public WorkloadListener Listener { get; set; }
        public List<WorkloadConsumer> Consumers { get; set; } = new List<WorkloadConsumer>();

        private bool forceStopped = false;
        private bool stopped = false;
        private bool disposed = false;
        private const int MAX_DISPOSE_TIMEOUT_SECONDS = 5;

        public WorkloadController()
        {
        }

        public void Run()
        {
            try
            {
				var startTime = Listener.StartAt;
				var endTime = DateTime.MaxValue;

                Listener.Initialize();

                logger.Info("Listener of type {ListenerTypeName} initialized correctly", Listener.GetType().Name);
                logger.Info("Event collection starts at {startTime}", startTime);
                // wait until Listener.StartAt has been reached
                while (DateTime.Now.CompareTo(startTime) < 0)
                {
                    Thread.Sleep(100);
                }

                logger.Info("Waiting for events");

                do
                {
                    try
                    {
                        if ((!Listener.IsRunning && Consumers.All(c => !c.HasMoreEvents())) || (endTime < DateTime.Now))
                        {
                            stopped = true;
                        }

                        if (endTime == DateTime.MaxValue && Listener.TimeoutMinutes != 0)
                        {
                            endTime = startTime.AddMinutes(Listener.TimeoutMinutes);
                        }

                        var evt = Listener.Read();
                        if (evt == null)
                        {
                            continue;
                        }

                        _ = Parallel.ForEach(Consumers, (cons) =>
                        {
                            cons.Consume(evt);
                        });
                    }
                    catch (Exception e)
                    {
                        logger.Error("Exception reading event");
                        logger.Error(e.Message);
                        logger.Error(e.StackTrace);
                    }
                } while (!stopped);

                // even when the listener has finished, wait until all buffered consumers are finished
                // unless the controller has been explicitly stopped by invoking Stop()
                // give max 1 minute grace time
                if (!forceStopped)
                {
                    var beginWait = DateTime.Now;
                    while (Consumers.Where(c => c is BufferedWorkloadConsumer).Any(c => c.HasMoreEvents()) && beginWait > DateTime.Now.AddMinutes(-1))
                    {
                        Thread.Sleep(10);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("Uncaught Exception");
                logger.Error(e.Message);
                logger.Error(e.StackTrace);

                var ex = e;
                while ((ex = ex.InnerException) != null){
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);
                }
            }
        }

        public void Stop()
        {
            forceStopped = true;
            stopped = true;
        }

        public void Dispose()
        {
            if (!disposed)
            {
                disposed = true;
                foreach (var cons in Consumers)
                {
                    cons?.Dispose();
                }

                Listener?.Dispose();
            }
        }

    }
}
