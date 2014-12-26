using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    internal class PollResult : TableEntity
    {
        #region Constructor
        public PollResult() { } // needed for TableEntity

        public PollResult(FeedId feedId)
        {
            PartitionKey = feedId;
            RowKey = C.CurrTimestamp();
        }
        #endregion

        #region Props
        public bool Success { get { return IsGood(); } }

        public HttpStatusCode StatusCode { get; set; }

        public FeedItem Content { get; set; }
        #endregion

        #region Methods
        public override string ToString()
        {
            return string.Join(C.SEPARATOR, PartitionKey, RowKey);
        }

        public static bool AreSame(PollResult a, PollResult b)
        {
            var same = false;

            same = (a != null && b != null);
            same = same && (a.PartitionKey == b.PartitionKey);
            same = same && (a.Content != null && b.Content != null);
            //same = same && (!string.IsNullOrWhiteSpace(a.Content.Content) && !string.IsNullOrWhiteSpace(b.Content.Content));
            //same = same && (a.Content.Content == b.Content.Content);
            same = same && (a.Content.ToString() == b.Content.ToString());

            return same;
        }

        #region Azure
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            foreach (var kv in properties)
            {
                switch (kv.Key.ToLower())
                {
                    case "code":
                        StatusCode = (HttpStatusCode)kv.Value.Int32Value;
                        break;

                    case "content":
                        Content = FeedItem.FromXml(kv.Value.StringValue);
                        break;

                    default:
                        break;
                }
            }
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = base.WriteEntity(operationContext);

            C.Extend(dict, "Code", new EntityProperty((int)StatusCode));
            C.Extend(dict, "Status", new EntityProperty(Enum.GetName(typeof(HttpStatusCode), StatusCode)));
            C.Extend(dict, "Content", new EntityProperty(Content.ToString()));

            return dict;
        }
        #endregion

        #endregion

        #region Subroutines
        private bool IsGood()
        {
            switch (StatusCode)
            {
                case HttpStatusCode.OK:
                case HttpStatusCode.NotModified:
                    return true;
            }

            return false;
        }
        #endregion
    }
}