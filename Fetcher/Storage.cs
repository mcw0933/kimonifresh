using System.Collections.Generic;
using Microsoft.WindowsAzure.ServiceRuntime;
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
        private static readonly CloudTable resultsTable;

        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit
        static Storage() {
            var connStr = RoleEnvironment.GetConfigurationSettingValue("StorageAccount"); // LocalAzureStorageEmulator_PortOverrides 
            azure = CloudStorageAccount.Parse(connStr);

            var queueName = RoleEnvironment.GetConfigurationSettingValue("PollRequestsQueue");
            queue = azure.CreateCloudQueueClient().GetQueueReference(queueName);

            var name = RoleEnvironment.GetConfigurationSettingValue("PollResultsTable");
            resultsTable = azure.CreateCloudTableClient().GetTableReference(name);
        } 

        private Storage() { }

        public static Storage _ { get { return instance; } }
        #endregion

        #region Methods
        internal IEnumerable<PollTarget> Query()
        {
            var name = RoleEnvironment.GetConfigurationSettingValue("PollTargetsTable");
            var table = azure.CreateCloudTableClient().GetTableReference(name);

            return table.ExecuteQuery<PollTarget>(new TableQuery<PollTarget>());
        }

        internal void AddMessage(PollTarget t, System.TimeSpan timeSpan)
        {
            var msg = new CloudQueueMessage(t.ToString());
            queue.AddMessage(msg, timeSpan);
        }

        internal int PeekMessages(int max)
        {
            return new List<CloudQueueMessage>(queue.PeekMessages(max)).Count;
        }

        internal IEnumerable<PollTarget> FetchMessages(int max)
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

        internal void Insert(PollResult result)
        {
            resultsTable.Execute(TableOperation.Insert(result));
        }
        #endregion

        
    }
}
