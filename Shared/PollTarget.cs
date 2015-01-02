using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Shared
{
    public class PollTarget : TableEntity
    {
        #region Constructor
        public PollTarget()
        {
            PartitionKey = Guid.NewGuid().ToString();
            RowKey = PartitionKey;
            Schedule = Schedule.HOURLY;
        }
        #endregion

        #region Props
        public FeedId Name { get { return PartitionKey; } set { if (!string.IsNullOrWhiteSpace(value)) PartitionKey = value; } }

        public Uri Source { get; set; }

        public Schedule Schedule { get; set; }

        public DateTimeOffset? NextRun { get; set; }
        #endregion

        #region Methods

        #region Azure
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            foreach (var kv in properties)
            {
                switch (kv.Key.ToLower())
                {
                    case "source":
                        Source = new Uri(kv.Value.StringValue);
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

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = base.WriteEntity(operationContext);

            C.Extend(dict, "Source", new EntityProperty(Source.AbsoluteUri));
            C.Extend(dict, "Schedule", new EntityProperty(Schedule.ToString()));
            C.Extend(dict, "NextRun", new EntityProperty(NextRun));

            return dict;
        }

        #endregion

        public override string ToString()
        {
            return string.Join(C.SEPARATOR, Name, Source, Schedule, NextRun);
        }

        public static PollTarget ParseFromString(string s)
        {
            var pieces = s.Split(C.SEPARATOR.ToCharArray());
            DateTimeOffset? nextRun = null;
            DateTimeOffset temp;
            if (DateTimeOffset.TryParse(pieces[3], out temp))
                nextRun = temp;

            return new PollTarget()
            {
                PartitionKey = pieces[0],
                RowKey = pieces[0],
                Source = new Uri(pieces[1]),
                Schedule = pieces[2],
                NextRun = nextRun
            };
        }
        #endregion
    }
}
