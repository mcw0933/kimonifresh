using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Quartz;
using Shared;

namespace Fetcher
{
    class PollJob : IInterruptableJob
    {
        #region Consts
        private const int TIMEOUT = 30 * C.TO_MILLI;  // fetch timeout
        private const int GULP_SIZE = 5;              // messages per run
        private const int CYCLE_TIME = 5;             // minutes to allow for a 
        #endregion

        internal static IJobDetail Create(string jobId, string group, string desc)
        {
            var job = JobBuilder.Create<PollJob>()
                    .WithIdentity(jobId, group)
                    .WithDescription(desc)
                    .UsingJobData(new JobDataMap())
                    .Build();

            return job;
        }

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                C.Log("Executing poll job...");

                var targets = GetTargetsForJobRun(GULP_SIZE);
                var nextCheck = PollForNewFeedContent(targets);

                Schedule.For(nextCheck, context);
            }
            catch (Exception ex)
            {
                C.Log("Error executing poll job: ", ex);
            }
        }

        public void Interrupt()
        {
            throw new NotImplementedException();
        }

        #region Subroutines

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

                #region Setup and execute tasks
                foreach (var msg in messages)
                {
                    if (msg.NextRun.HasValue && msg.NextRun.Value > now)
                        heldMsgs.Add(msg.NextRun.Value, msg);
                    else
                    {
                        var t = Task.Factory.StartNew(() =>
                        {
                            C.Log("Setting up a fetch for {0} ({1})...", msg.Name, msg.Source);
                            var result = FetchUrl(msg);

                            if (!result.Success || Unchanged(result))
                            {
                                // on schedule, hasn't changed, try again in a little while.
                                msg.NextRun = Time.Mins(60);

                                C.Log("Target {0}, will try again at {1}", (result.Success) ? "was unchanged" : "failed", C.Localize(msg.NextRun));
                                heldMsgs.Add(msg.NextRun.Value, msg);
                            }
                            else
                            {
                                C.Log("Got a new content item for {0} ({1})!  Saving to feed...", msg.Name, msg.Source);
                                SaveResult(msg, result);
                            }
                        });

                        tasks.Add(t);
                    }
                }

                if (tasks.Count > 0)
                    Task.WaitAll(tasks.ToArray());
                #endregion

                if (heldMsgs.Count > 0) // 
                {
                    Storage._.SaveMessages(heldMsgs.Values);
                    nextCheck = heldMsgs.Keys[0];
                }
            }

            return nextCheck;
        }

        private PollResult FetchUrl(PollTarget item)
        {
            var result = new PollResult(item.Name);
            var statusCode = string.Empty;

            try
            {
                var target = item.Source;

                if (target.Host == Kimono.Host)
                {
                    var uri = new UriBuilder(target);
                    uri.Query = "apikey=" + Kimono.Key;
                    target = uri.Uri;
                }

                C.Log("Issuing request to {0}...", target);
                var req = (HttpWebRequest)WebRequest.Create(target);
                req.Timeout = TIMEOUT;
                req.Method = "GET";

                using (var resp = (HttpWebResponse)req.GetResponse())
                {
                    result.StatusCode = resp.StatusCode;

                    C.Log("Got response with status code {0} ({1})", (int)result.StatusCode, Enum.GetName(typeof(HttpStatusCode), result.StatusCode));
                    if (result.Success)
                    {
                        result.Content = Feed.ProcessJsonStream(result.RowKey, GetTransformFor(result.PartitionKey), resp.GetResponseStream());

                        if (result.Content == null)
                        {
                            result.StatusCode = HttpStatusCode.NoContent;
                            item.NextRun = Time.Mins(60);

                            C.Log("Failed to process valid json for {0}, will try again at {1}.", target.AbsoluteUri, C.Localize(item.NextRun));
                        }
                        else
                        {
                            item.NextRun = (result.Content.NextPollAfter > C.CurrTime()) ?
                                result.Content.NextPollAfter :
                                Time.NextRunFor(item.Schedule);

                            C.Log("Processed response successfully!  Setting next run for {0}...", C.Localize(item.NextRun));
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                C.Log("Error getting content for url {0}: ", ex, item.Source);
            }

            return result;
        }

        private Func<ItemId, JObject, FeedItem> GetTransformFor(ItemId feedId)
        {
            // TODO: implement dynamic load
            return Transformer.DefaultTransform;
        }

        private bool Unchanged(PollResult result)
        {
            var last = Storage._.GetLast(result.PartitionKey);
            return PollResult.AreSame(last, result);
        }

        private void SaveResult(PollTarget target, PollResult result)
        {
            Storage._.Update(target);

            Storage._.Insert(result);

            Storage._.AppendToFeed(result);
        }
        #endregion
    }
}
