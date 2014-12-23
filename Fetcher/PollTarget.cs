using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    internal class PollTarget : TableEntity
    {
        #region Consts
        private static readonly string PARTITION_KEY = "PollTarget";
        #endregion

        #region Constructor
        public PollTarget()
        {
            PartitionKey = PARTITION_KEY;
            RowKey = Guid.NewGuid().ToString();
            Schedule = Schedule.HOURLY;
        }
        #endregion

        #region Props
        public Uri Uri { get; set; }

        public Schedule Schedule { get; set; }

        public DateTimeOffset? NextRun { get; set; }
        #endregion

        #region Methods

        #region Azure
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            foreach (var kv in properties)
            {
                switch (kv.Key.ToLower())
                {
                    case "uri":
                        Uri = new Uri(kv.Value.StringValue);
                        break;

                    case "schedule":
                        Schedule = kv.Value.StringValue;
                        break;

                    case "nextrun":
                        NextRun = kv.Value.DateTimeOffsetValue;
                        break;

                    default:
                        break;
                }
            }
        }
        #endregion

        public override string ToString()
        {
            return string.Join(C.SEPARATOR, Uri, Schedule, NextRun, RowKey);
        }

        public static PollTarget ParseFromString(string s)
        {
            var pieces = s.Split(C.SEPARATOR.ToCharArray());
            DateTimeOffset? nextRun = null;
            DateTimeOffset temp;
            if (DateTimeOffset.TryParse(pieces[2], out temp))
                nextRun = temp;

            return new PollTarget()
            {
                PartitionKey = PARTITION_KEY,
                Uri = new Uri(pieces[0]),
                Schedule = pieces[1],
                NextRun = nextRun,
                RowKey = pieces[3]
            };
        }
        #endregion
    }
}
