using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Quartz;

namespace Fetcher
{
    class PollJob : IInterruptableJob
    {
        #region Consts
        private const int MSEC = 1000;          // milli
        private const int TIMEOUT = 30 * MSEC;  // fetch timeout
        private const int GULP_SIZE = 5;      // messages per run
        private const int CYCLE_TIME = 5;       // minutes to allow for a 
        #endregion

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                C.Log("Executing poll job...");

                // TODO: Move to service initialization, or at least to a separate schedule.
                WriteInitialTargets();

                var targets = GetTargetsForJobRun(GULP_SIZE);
                var nextCheck = PollForNewFeedContent(targets);

                Schedule.For(nextCheck, context);
            }
            catch (Exception ex)
            {
                C.Log("Error executing job: ", ex);
            }
        }

        public void Interrupt()
        {
            throw new NotImplementedException();
        }

        #region Subroutines

        private void WriteInitialTargets()
        {
            if (Lock._.AcquireLease())
            {
                C.Log("Lease acquired, role instance: {0}", C.Id);

                var targets = ReadMasterData();
                var numTargets = targets.Count;

                var msgCount = Storage._.PeekMessages(targets.Count);

                if (msgCount < numTargets)
                    C.Log("Not all targets are presently queued.");

                if (msgCount == 0)
                    // TODO: Update this to only requeue missing items.
                    SaveMessages(targets);
            }
            else
            {
                C.Log("Failed to acquire lease, role instance: {0}", C.Id);
                Thread.Sleep(15 * MSEC);
            }
        }

        private List<PollTarget> ReadMasterData()
        {
            var queryResult = Storage._.Query();
            return queryResult.ToList();
        }

        private void SaveMessages(IEnumerable<PollTarget> targets)
        {
            foreach (var t in targets)
                Storage._.AddMessage(t, TimeSpan.FromSeconds(45));
        }

        private List<PollTarget> GetTargetsForJobRun(int max)
        {
            // TODO: Rewrite such that a message isn't totally popped
            // unless it succeeds (or gets put back if fails, etc)
            return new List<PollTarget>(Storage._.FetchMessages(max));
        }

        private DateTimeOffset PollForNewFeedContent(List<PollTarget> messages)
        {
            var nextCheck = Time.Days(1);
            var ct = messages.Count;

            if (ct > 0)
            {
                var now = C.CurrTime();

                var tasks = new List<Task>(ct);
                var heldMsgs = new SortedList<DateTimeOffset, PollTarget>(ct);

                foreach (var msg in messages)
                {
                    if (msg.NextRun.HasValue && msg.NextRun.Value > now)
                        heldMsgs.Add(msg.NextRun.Value, msg);
                    else
                    {
                        var t = Task.Factory.StartNew(() =>
                        {
                            var result = FetchUrl(msg);
                            SaveResult(result);
                        });

                        tasks.Add(t);
                    }
                }

                if (heldMsgs.Count > 0)
                {
                    SaveMessages(heldMsgs.Values);
                    nextCheck = heldMsgs.Values[0].NextRun.Value;
                }

                if (tasks.Count > 0)
                    Task.WaitAll(tasks.ToArray());
            }

            return nextCheck;
        }

        private PollResult FetchUrl(PollTarget item)
        {
            var result = new PollResult()
            {
                PartitionKey = item.RowKey,
                RowKey = C.CurrTimestamp(),
                Uri = item.Uri
            };

            var statusCode = string.Empty;

            try
            {
                var target = item.Uri;

                if (target.Host == Kimono.Host) {
                    var uri =new UriBuilder(target);
                    uri.Query = "apikey=" + Kimono.Key;
                    target = uri.Uri;
                }

                var req = (HttpWebRequest)WebRequest.Create(target);
                req.Timeout = TIMEOUT;
                req.Method = "GET";

                using (var resp = (HttpWebResponse)req.GetResponse())
                {
                    result.StatusCode = resp.StatusCode;

                    if (result.Success)
                    {
                        result.Content = Feed.ProcessJsonStream(result.RowKey, GetTransformFor(result.PartitionKey), resp.GetResponseStream());

                        item.NextRun = (result.Content.NextPollAfter > C.CurrTime()) ?
                            result.Content.NextPollAfter :
                            Time.NextRunFor(item.Schedule);
                    }

                }
            }
            catch (Exception ex)
            {
                C.Log("Error getting content for url {0}: ", ex, result.Uri);
            }

            return result;
        }

        private Func<ItemId, JObject, FeedItem> GetTransformFor(ItemId feedId)
        {
            // TODO: implement dynamic load
            return Transformer.DefaultTransform;
        }

        private void SaveResult(PollResult result)
        {
            Storage._.Insert(result);
        }
        #endregion
    }
}
