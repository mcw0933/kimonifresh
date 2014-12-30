using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Table;

namespace Fetcher
{
    public class C
    {
        internal static readonly string SEPARATOR = "\n";
        internal static readonly TimeSpan OFFSET = new TimeSpan(-5, 0, 0);
        public const int TO_MILLI = 1000;

        public static string Setting(string name)
        {
            return RoleEnvironment.GetConfigurationSettingValue(name);
        }

        internal static string FormatErrorMsg(Exception ex)
        {
            return ex.Message;
        }

        public static DateTimeOffset CurrTime()
        {
            return Localize(DateTimeOffset.UtcNow);
        }

        internal static string CurrTimestamp()
        {
            return DateTimeOffset.UtcNow.Ticks.ToString("d19");
        }

        internal static DateTimeOffset Localize(DateTimeOffset dt)
        {
            return dt.ToOffset(OFFSET);
        }

        public static void Log(string message, Exception ex, params object[] values)
        {
            Log(message + C.FormatErrorMsg(ex), values);
        }

        public static void Log(string message, params object[] values)
        {
            var logFormat = string.Format("[{0}] - {1}", C.CurrTime(), message);
            //Console.WriteLine(logFormat, values);

            var logMsg = string.Format(logFormat, values);
            Trace.WriteLine(logMsg, "Log");
        }

        public static string Id { get { return RoleEnvironment.CurrentRoleInstance.Id; } }

        internal static void Extend(IDictionary<string, EntityProperty> dict, string name, EntityProperty prop)
        {
            if (dict.ContainsKey(name))
                dict[name] = prop;
            else
                dict.Add(name, prop);
        }
    }

    public class Kimono
    {
        public static string Host { get { return C.Setting("KimonoHostName"); } }
        public static string Key { get { return C.Setting("KimonoAPIKey"); } }
    }
}
