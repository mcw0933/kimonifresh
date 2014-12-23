using System;
using System.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
namespace Fetcher
{
    class C
    {
        internal static readonly string SEPARATOR = "\r\n";
        internal static readonly TimeSpan OFFSET = new TimeSpan(-5, 0, 0);

        internal static string FormatErrorMsg(Exception ex)
        {
            return ex.Message;
        }

        internal static DateTimeOffset CurrTime()
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

        internal static void Log(string message, Exception ex, params object[] values)
        {
            Log(message + C.FormatErrorMsg(ex), values);
        }

        internal static void Log(string message, params object[] values)
        {
            var logFormat = string.Format("[{0}] - {1}", C.CurrTime(), message);
            //Console.WriteLine(logFormat, values);

            var logMsg = string.Format(logFormat, values);
            Trace.WriteLine(logMsg, "Log");
        }

        public static string Id { get { return RoleEnvironment.CurrentRoleInstance.Id; } }
    }

    class Kimono
    {
        public static string Host { get { return RoleEnvironment.GetConfigurationSettingValue("KimonoHostName"); } }
        public static string Key { get { return RoleEnvironment.GetConfigurationSettingValue("KimonoAPIKey"); } }
    }
}
