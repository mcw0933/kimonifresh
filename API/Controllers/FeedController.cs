using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Web.Http;
using Fetcher;

namespace API.Controllers
{
    public class FeedController : ApiController
    {
        public HttpResponseMessage Get()
        {
            var content = string.Join("<p>", _Get());

            return new HttpResponseMessage() { Content = new StringContent(content, Encoding.UTF8, "text/html") };
        }
        
        private IEnumerable<string> _Get()
        {
            //var routeBase = HttpContext.Current.Request.RequestContext.RouteData.Route;
            var urlBase = Request.RequestUri.GetLeftPart(UriPartial.Path).TrimEnd('/');

            foreach (var f in Storage._.Query())
                yield return string.Format("<a href='{0}/{1}'>{1}</a>", urlBase, f.Name);
        }

        public HttpResponseMessage Get(string id)
        {
            var list = new List<FeedItem>();

            foreach (var f in Storage._.Read(id))
                list.Add(f.Content);

            var feed = Feed.CreateFrom(list);

            return new HttpResponseMessage() { Content = new StringContent(feed.ToString(), Encoding.UTF8, "application/atom+xml") };
        }

        public void Post([FromBody]string value)
        {
        }

        public void Put(int id, [FromBody]string value)
        {
        }

        public void Delete(int id)
        {
        }
    }
}
