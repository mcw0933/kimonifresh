using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    public class PollResult : TableEntity
    {
        public Uri Uri { get; set; }

        //public DateTime Timestamp { get; set; }

        public object StatusCode { get; set; }

        public object Content { get; set; }

        public override string ToString()
        {
            return Uri + C.SEPARATOR + C.Localize(Timestamp);
        }
    }
}