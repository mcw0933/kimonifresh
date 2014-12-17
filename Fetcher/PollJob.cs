using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Quartz;

namespace Fetcher
{
    class PollJob : IInterruptableJob
    {
        public void Execute(IJobExecutionContext context)
        {
            C.Log("Executing poll job...");

            if (Lock._.AcquireLease())
            {
                C.Log("Lease acquired, role instance: {0}", C.Id);

                var targets = ReadMasterData();
                SaveMessages(targets);
            }
            else
            {
                C.Log("Failed to acquire lease, role instance: {0}", C.Id);
                Thread.Sleep(15 * 1000);
            }

            var messages = FetchMessages(5); // Now we'll fetch 5 messages from top of queue
            var ct = messages.Count;

            if (ct > 0)
            {
                var tasks = new Task[ct];
                
                for (var ix = 0; ix < ct; ix++)
                {
                    tasks[ix] = Task.Factory.StartNew(() =>
                    {
                        var result = FetchUrl(messages[ix]);
                        SaveResult(result);
                    });
                }

                Task.WaitAll(tasks);
            }
        }

        public void Interrupt()
        {
            throw new NotImplementedException();
        }

        #region Subroutines
        private List<PollTarget> ReadMasterData()
        {
            var queryResult = Storage._.Query();
            return queryResult.ToList();
        }

        private void SaveMessages(List<PollTarget> targets)
        {
            foreach (var t in targets)
            {
                Storage._.AddMessage(t, TimeSpan.FromSeconds(45));
            }
        }

        private List<PollTarget> FetchMessages(int max)
        {
            return new List<PollTarget>(Storage._.FetchMessages(max));
        }

        private PollResult FetchUrl(PollTarget item)
        {
            var startDateTime = DateTime.UtcNow;
            var elapsedTime = TimeSpan.FromSeconds(0);
            var statusCode = "";
            var contentLength = 0D;
            try
            {
                var req = (HttpWebRequest)WebRequest.Create(item.Uri);
                req.Timeout = 30 * 1000;//Let's timeout the request in 30 seconds.
                req.Method = "GET";
                using (var resp = (HttpWebResponse)req.GetResponse())
                {
                    DateTime endDateTime = DateTime.UtcNow;
                    elapsedTime = new TimeSpan(endDateTime.Ticks - startDateTime.Ticks);
                    statusCode = resp.StatusCode.ToString();
                    contentLength = resp.ContentLength;
                }
            }
            catch (WebException ex)
            {
                DateTime endDateTime = DateTime.UtcNow;
                elapsedTime = new TimeSpan(endDateTime.Ticks - startDateTime.Ticks);
                statusCode = ex.Status.ToString();
            }

            return new PollResult()
            {
                PartitionKey = DateTime.UtcNow.Ticks.ToString("d19"),
                RowKey = item.RowKey,
                Uri = item.Uri,
                StatusCode = statusCode,
                //ContentLength = contentLength,
                //TimeTaken = elapsedTime.TotalMilliseconds,
            };
        }

        private void SaveResult(PollResult result)
        {
            Storage._.Insert(result);
        }
        #endregion
    }
}
