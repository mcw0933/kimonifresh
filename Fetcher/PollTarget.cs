using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    public class PollTarget : TableEntity
    {
        public PollTarget()
        {
            PartitionKey = "PollTarget";
            RowKey = Guid.NewGuid().ToString();
            Schedule = "Hourly";
        }

        public Uri Uri { get; set; }

        public string Schedule { get; set; }

        public override string ToString()
        {
            return Uri + C.SEPARATOR + RowKey;
        }

        public static PollTarget ParseFromString(string s)
        {
            string[] splitter = { C.SEPARATOR };
            string[] pieces = s.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
            return new PollTarget()
            {
                PartitionKey = "PingItem",
                Uri = new Uri(pieces[0]),
                RowKey = pieces[1]
            };
        }
    }
}
