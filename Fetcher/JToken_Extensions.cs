using System;
using Newtonsoft.Json.Linq;

namespace Fetcher
{
    static class JToken_Extensions
    {
        public static string Str(this JToken j, string field)
        {
            return j.Get<string>(field, string.Empty);
        }

        public static DateTimeOffset Dto(this JToken j, string field)
        {
            var str = Str(j, field);
            DateTimeOffset dt;

            if (!string.IsNullOrWhiteSpace(str) && DateTimeOffset.TryParse(str, out dt))
                return dt;

            return DateTimeOffset.MinValue;
        }

        public static T Get<T>(this JToken j, string field, T defVal) where T : class {
            T t = null;
            
            var f = j[field];
            if (f != null)
                t = f.Value<T>();

            return t ?? defVal;
        }
    }
}
