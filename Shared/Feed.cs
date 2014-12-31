using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;
using Argotic.Common;
using Argotic.Syndication;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Fetcher
{
    public class Feed
    {
        private AtomFeed feed = new AtomFeed();

        public static FeedItem ProcessJsonStream(ItemId id, Func<ItemId, JObject, FeedItem> transform, Stream stream)
        {
            try
            {
                using (var sr = new StreamReader(stream))
                {
                    try
                    {
                        using (var jtr = new JsonTextReader(sr))
                        {
                            JObject json;
                            try
                            {
                                json = JObject.Load(jtr);
                            }
                            catch (Exception ex)
                            {
                                C.Log("Error loading json text: ", ex);
                                return null;
                            }

                            return transform(id, json);
                        }
                    }
                    catch (Exception ex)
                    {
                        C.Log("Error reading json text: ", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                C.Log("Error reading stream: ", ex);
            }

            return null;
        }

        public static Feed CreateFrom(IEnumerable<FeedItem> items)
        {
            var feed = new Feed();

            foreach (var item in items)
                feed.feed.AddEntry(item.Entry);

            return feed;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            using (var xw = XmlWriter.Create(sb, new XmlWriterSettings() { Indent = true }))
            {
                feed.Save(xw, new SyndicationResourceSaveSettings() { CharacterEncoding = Encoding.UTF8 });
            }

            return sb.ToString();
        }

        public static Feed FromXml(string xml)
        {
            var feed = new Feed();

            using (var sr = new StringReader(xml))
            {
                using (var xr = XmlReader.Create(sr))
                {
                    feed.feed.Load(xr);
                }
            }

            return feed;
        }
    }

    public class FeedId
    {
        private string id = string.Empty;

        public static implicit operator FeedId(string s)
        {
            return new FeedId() { id = s };
        }

        public static implicit operator string(FeedId i)
        {
            return i.id;
        }

        public override string ToString()
        {
            return id;
        }
    }
}
