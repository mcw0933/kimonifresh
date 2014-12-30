using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Fetcher
{
    public class Feed
    {
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
