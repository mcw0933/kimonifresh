using System;
using System.Collections.Generic;
using System.Threading;
using Quartz;

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
                    // try firing the poll job now
                    Schedule.FireNow(context.Scheduler, "PollJob:" + C.Id);
                }
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

                var msgs = Storage._.PeekMessages(targets.Count);

                if (msgs.Count < numTargets)
                {
                    C.Log("Not all targets are presently queued - queueing all missing targets.");

                    IList<PollTarget> requeue;

                    if (msgs.Count == 0)
                        requeue = targets;
                    else
                    {
                        requeue = new List<PollTarget>();
                        foreach (var m in msgs)
                        {
                            var a = PollTarget.ParseFromString(m.AsString);
                            var found = false;

                            foreach (var b in targets)
                                if (a.PartitionKey == b.PartitionKey)
                                    found = true;

                            if (!found)
                                requeue.Add(a);
                        }
                    }

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
