using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    internal class PollResult : TableEntity
    {
        public Uri Uri { get; set; }

        public bool Success { get { return IsGood(); } }

        public HttpStatusCode StatusCode { get; set; }

        public FeedItem Content { get; set; }

        public override string ToString()
        {
            return string.Join(C.SEPARATOR, Uri, C.Localize(Timestamp));
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = new Dictionary<string, EntityProperty>(3) {
                { "Uri", new EntityProperty(Uri.AbsoluteUri) },
                { "Code", new EntityProperty((int)StatusCode) },
                { "Status", new EntityProperty(Enum.GetName(typeof(HttpStatusCode), StatusCode)) },
                { "Content", new EntityProperty(Content.ToString()) }
            };

            return dict;
        }

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
    }
}