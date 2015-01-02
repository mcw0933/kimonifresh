using System;
using System.Collections.Generic;
using System.Threading;
using Quartz;
using Shared;

namespace Fetcher
{
    class CheckJob : IInterruptableJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                C.Log("Executing check job...");

                if (WriteInitialTargets() > 0)
                {
                    C.Log("Wrote some messages, trying to fire a poll to address them...");

                    var jobId = "PollJob:" + C.Id;
                    var group = C.Setting("JobGroup");

                    // try firing the poll job now
                    Schedule.FireNow(context.Scheduler, jobId);
                    
                    //var job = PollJob.Create(jobId, group, "Onetime poll spawned by check");

                    //var trigger = TriggerBuilder.Create()
                    //    .WithIdentity(jobId, group)
                    //    .StartNow()
                    //    .Build();

                    //context.Scheduler.ScheduleJob(job, trigger);
                }
                else
                    C.Log("No work to do, all expected messages are queued and waiting...");

                C.Log("Finished check, will check again at {0}...", C.Localize(context.Trigger.GetNextFireTimeUtc()));
            }
            catch (Exception ex)
            {
                C.Log("Error executing check job: ", ex);
            }
        }

        public void Interrupt()
        {
            throw new NotImplementedException();
        }

        #region Methods

        internal static int WriteInitialTargets()
        {
            var newMsgCt = 0;

            if (Lock._.AcquireLease())
            {
                C.Log("Lease acquired, role instance: {0}", C.Id);

                var targets = Storage._.Query();
                var numTargets = targets.Count;

                C.Log("Checking queue for existing messages...");

                var msgs = Storage._.PeekMessages(targets.Count);

                if (msgs.Count < numTargets)
                {
                    C.Log("Found {0} messages for {1} targets.  Not all targets are presently queued - queueing all missing targets.", msgs.Count, numTargets);

                    IList<PollTarget> requeue;

                    if (msgs.Count == 0)
                        requeue = targets;
                    else
                    {
                        requeue = new List<PollTarget>();

                        foreach (var b in targets)
                        {
                            var found = false;

                            foreach (var m in msgs)
                            {
                                var a = PollTarget.ParseFromString(m.AsString);
                                if (a.PartitionKey == b.PartitionKey)
                                    found = true;
                            }

                            if (!found)
                                requeue.Add(b);
                        }
                    }

                    C.Log("Writing {0} messages...", requeue.Count);

                    Storage._.SaveMessages(requeue);
                    newMsgCt = requeue.Count;
                }
            }
            else
            {
                C.Log("Failed to acquire lease, role instance: {0}", C.Id);

                // wait a little while since another role has the lease and may be writing messages now.
                Thread.Sleep(15 * C.TO_MILLI);
            }

            return newMsgCt;
        }

        #endregion
    }
}
