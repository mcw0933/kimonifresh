using System;
using Newtonsoft.Json.Linq;

namespace Fetcher
{
    internal class Transformer
    {
        internal static FeedItem DefaultTransform(ItemId id, JObject wrapper)
        {
            try
            {
                var post = wrapper["results"]["comic"];

                if (post.Type == JTokenType.Array)
                    post = post[0];

                if (post.HasValues)
                    return new FeedItem()
                    {
                        //Id = id,
                        Uri = new Uri(post.Str("url")),
                        Title = post.Str("title"),
                        //Authors = post.Str("authors").Split(','),
                        Content = FormatImage(post["image"]),
                        PublishTime = post.Dto("date"),
                        LastUpdateTime = wrapper.Dto("lastsuccess"),
                        NextPollAfter = wrapper.Dto("nextrun")
                    };
            }
            catch (Exception ex)
            {
                C.Log("Error transforming json text {0}: ", ex, wrapper);
            }
            return null;
        }

        private static string FormatImage(JToken image)
        {
            return string.Format("<img src='{0}' alt='{1}' title='{2}' />",
                image.Str("src"),
                image.Str("alt"),
                image.Str("title"));
        }
    }
}
