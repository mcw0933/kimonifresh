using System.Diagnostics;
using Shared;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace API
{
    public class WebRole : RoleEntryPoint
    {
        public override bool OnStart()
        {
            C.Log("Starting web role, switching to warning logging only...");
            RoleEnvironment.TraceSource.Switch.Level = SourceLevels.Warning;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }
    }
}
