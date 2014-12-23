using System;
using System.IO;
using System.Text;
using System.Xml;
using Argotic.Common;
using Argotic.Syndication;

namespace Fetcher
{
    internal class FeedItem
    {
        private AtomEntry entry;

        public FeedItem() {
            entry = new AtomEntry();
        }

        public ItemId Id
        {
            get { return Uri.ToString(); }
        }

        public Uri Uri {
            get { return entry.BaseUri; }
            set { entry.BaseUri = value; }
        }

        public string Title {
            get { return (entry.Title == null) ? string.Empty : entry.Title.Content; }
            set { entry.Title = new AtomTextConstruct(value); }
        }

        public string Content {
            get { return (entry.Content == null) ? string.Empty : entry.Content.Content; }
            set { entry.Content = new AtomContent(value); }
        }

        public DateTimeOffset PublishTime {
            get { return entry.PublishedOn; }
            set { entry.PublishedOn = (value == DateTimeOffset.MinValue || value == DateTimeOffset.MaxValue) ? value.UtcDateTime : C.CurrTime().UtcDateTime; }
        }

        public DateTimeOffset LastUpdateTime {
            get { return entry.UpdatedOn; }
            set { entry.UpdatedOn = (value == DateTimeOffset.MinValue || value == DateTimeOffset.MaxValue) ? value.UtcDateTime : C.CurrTime().UtcDateTime; }
        }

        public DateTimeOffset NextPollAfter { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            using (var xw = XmlWriter.Create(sb, new XmlWriterSettings() { Indent = true })) {
                entry.Save(xw, new SyndicationResourceSaveSettings() { CharacterEncoding = Encoding.UTF8 });
            }

            return sb.ToString();
        }
    }

    internal class ItemId
    {
        private string id = string.Empty;

        public static implicit operator ItemId(string s)
            {
                return new ItemId() { id = s };
            }

        public override string ToString()
        {
            return id;
        }
    }
}
