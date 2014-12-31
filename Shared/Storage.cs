using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    public sealed class Storage
    {
        #region Singleton implementation
        private static readonly Storage instance = new Storage();
        private static readonly CloudStorageAccount azure;
        private static readonly CloudQueue queue;
        private static readonly CloudTable targetsTable;
        private static readonly CloudTable resultsTable;
        private static readonly CloudTable lastResultTable;

        public static readonly TimeSpan QUEUE_TTL = TimeSpan.FromSeconds(60);

        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit
        static Storage() {
            var connStr = C.Setting("StorageAccount"); // LocalAzureStorageEmulator_PortOverrides 
            azure = CloudStorageAccount.Parse(connStr);

            var queueName = C.Setting("PollRequestsQueue");
            queue = azure.CreateCloudQueueClient().GetQueueReference(queueName);
            queue.CreateIfNotExists();

            var name = C.Setting("PollTargetsTable");
            targetsTable = azure.CreateCloudTableClient().GetTableReference(name);
            targetsTable.CreateIfNotExists();

            name = C.Setting("PollResultsTable");
            resultsTable = azure.CreateCloudTableClient().GetTableReference(name);
            resultsTable.CreateIfNotExists();

            name = C.Setting("LastResultTable");
            lastResultTable = azure.CreateCloudTableClient().GetTableReference(name);
            lastResultTable.CreateIfNotExists();
        } 

        private Storage() { }

        public static Storage _ { get { return instance; } }
        #endregion

        #region Methods

        #region Targets - queue and table
        public IList<PollTarget> Query()
        {
            return targetsTable.ExecuteQuery<PollTarget>(new TableQuery<PollTarget>()).ToList();
        }

        public void SaveMessages(IEnumerable<PollTarget> targets)
        {
            foreach (var t in targets)
                AddMessage(t, (t.NextRun.HasValue) ? 
                    t.NextRun.Value.AddMinutes(1) - C.CurrTime() : 
                    QUEUE_TTL);
        }

        public void AddMessage(PollTarget t, TimeSpan ttl)
        {
            var msg = new CloudQueueMessage(t.ToString());
            queue.AddMessage(msg, ttl);
        }

        public IList<CloudQueueMessage> PeekMessages(int max)
        {
            return new List<CloudQueueMessage>(queue.PeekMessages(max));
        }

        public IEnumerable<PollTarget> FetchMessages(int max)
        {
            // TODO: Rewrite such that a message isn't totally popped
            // unless it succeeds (or gets put back if fails, etc)
            var messages = queue.GetMessages(max);

            var targets = new List<PollTarget>();
            foreach (var m in messages)
            {
                targets.Add(PollTarget.ParseFromString(m.AsString));
                queue.DeleteMessage(m);
            }

            return targets;
        }

        public void Update(PollTarget target)
        {
            target.ETag = "*";
            var op = TableOperation.Replace(target);
            targetsTable.Execute(op);
        }
        #endregion

        #region Results - table and feed

        public PollResult GetLast(string pKey)
        {
            var op = TableOperation.Retrieve<PollResult>(pKey, pKey);
            var result = lastResultTable.Execute(op).Result as PollResult;

            return result;
        }

        public void AppendToFeed(PollResult result)
        {
            // TODO: implement here?  Implement in API?
        }

        public void Insert(PollResult result)
        {
            // update results table
            resultsTable.Execute(TableOperation.Insert(result));

            // overwrite latest result
            var last = GetLast(result.PartitionKey);

            if (last == null)
                last = new PollResult(result.PartitionKey)
                {
                    RowKey = result.PartitionKey,
                    StatusCode = result.StatusCode,
                    Content = FeedItem.FromXml(result.Content.ToString())
                };

            var op = TableOperation.InsertOrReplace(last);
            lastResultTable.Execute(op);
        }

        public IList<PollResult> Read(FeedId feed)
        {
            var query = resultsTable.CreateQuery<PollResult>().Where(f => f.PartitionKey == feed.ToString());
            return query.ToList();
        }
        #endregion

        #endregion
    }
}
