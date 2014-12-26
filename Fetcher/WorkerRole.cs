using System;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using Quartz;
using Quartz.Impl;

namespace Fetcher
{
    public class WorkerRole : RoleEntryPoint
    {
        #region Members
        ManualResetEvent completed = new ManualResetEvent(false);
        IScheduler sched = new StdSchedulerFactory().GetScheduler();
        #endregion

        // TO CONFIGURE DIAGNOSTICS:
        // http://azure.microsoft.com/en-us/documentation/articles/cloud-services-dotnet-diagnostics/
        // Right-click the role (under Roles), Properties - Configuration, check Enable Diagnostics, provide
        // a ConnectionString (or use UseDevelopmentStorage=true)

        public override void Run()
        {
            C.Log("Starting processing of messages...");

            var group = C.Setting("JobGroup");
            var id = C.Id;

            sched.Start();

            RunPollJob(group, id);
            RunTargetCheckJob(group, id);

            // once scheduled, the job will periodically reschedule itself,
            // so the main process loop is between sched and PollJob.Execute

            completed.WaitOne();
        }

        private void RunPollJob(string group, string id)
        {
            if (sched.IsStarted)
            {
                var job = JobBuilder.Create<PollJob>()
                        .WithIdentity("PollJob:" + id, group)
                        .WithDescription("Source Feed Targets Poll Job")
                        .UsingJobData(new JobDataMap())
                        .Build();

                var trigger = TriggerBuilder.Create()
                    .WithIdentity("PollJob:" + id, group)
                    .WithCronSchedule(C.Setting("Minutely"))
                    .StartNow()
                    .Build();

                sched.ScheduleJob(job, trigger);
            }
            else
                C.Log("Scheduler has not started!");
        }

        private void RunTargetCheckJob(string group, string id)
        {
            if (sched.IsStarted)
            {
                var job = JobBuilder.Create<CheckJob>()
                        .WithIdentity("CheckJob:" + id, group)
                        .WithDescription("New Source Feed Targets Check Job")
                        .UsingJobData(new JobDataMap())
                        .Build();

                var trigger = TriggerBuilder.Create()
                    .WithIdentity("CheckJob:" + id, group)
                    .WithCronSchedule(C.Setting("TargetCheckSchedule"))
                    .StartNow()
                    .Build();

                sched.ScheduleJob(job, trigger);
            }
            else
                C.Log("Scheduler has not started!");
        }

        #region Start / Stop

        public override bool OnStart()
        {
            var ready = false;

            try
            {
                if (base.OnStart())
                {
                    Lock.Init();
                    CheckJob.WriteInitialTargets();
                    ready = true;
                }
                else
                    C.Log("Base role failed to start!");
            }
            catch (Exception ex)
            {
                C.Log("Error during startup: ", ex);
            }

            return ready;
        }

        public override void OnStop()
        {
            try
            {
                completed.Set();
                sched.Shutdown(false);
                base.OnStop();
            }
            catch (Exception ex)
            {
                C.Log("Error during shutdown: ", ex);
            }
        }

        #endregion
    }
}
