using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using Quartz;
using Quartz.Impl;
using Shared;

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

            C.Log("Role has started to run, setting up job scheduler...");
            sched.Start();

            C.Log("Setting up jobs...");
            RunPollJob(group, id);
            RunTargetCheckJob(group, id);

            // once scheduled, the job will periodically reschedule itself,
            // so the main process loop is between sched and PollJob.Execute

            C.Log("Jobs are set.");
            completed.WaitOne();
        }

        private void RunPollJob(string group, string id)
        {
            if (sched.IsStarted)
            {
                var jobId = "PollJob:" + id;
                var desc = "Source Feed Targets Poll Job";
                var freq = C.Setting("Minutely");

                C.Log("Setting up poll job running with schedule {3} in group {0} with id {1} and description: {2}", group, jobId, desc, freq);

                var job = PollJob.Create(jobId, group, desc);

                var trigger = TriggerBuilder.Create()
                    .WithIdentity(jobId, group)
                    .WithCronSchedule(freq)
                    .StartNow()
                    .Build();

                C.Log("Job set to start at {0}, with next scheduled fire at {1}...", C.Localize(trigger.StartTimeUtc), C.Localize(trigger.GetNextFireTimeUtc()));

                sched.ScheduleJob(job, trigger);
            }
            else
                C.Log("Scheduler has not started!");
        }

        

        private void RunTargetCheckJob(string group, string id)
        {
            if (sched.IsStarted)
            {
                var jobId = "CheckJob:" + id;
                var desc = "New Source Feed Targets Check Job";
                var freq = C.Setting("TargetCheckSchedule");

                C.Log("Setting up check job running with schedule {3} in group {0} with id {1} and description: {2}", group, jobId, desc, freq);

                var job = JobBuilder.Create<CheckJob>()
                        .WithIdentity(jobId, group)
                        .WithDescription(desc)
                        .UsingJobData(new JobDataMap())
                        .Build();

                var trigger = TriggerBuilder.Create()
                    .WithIdentity(jobId, group)
                    .WithCronSchedule(freq)
                    .StartNow()
                    .Build();

                C.Log("Job set to start at {0}, with next scheduled fire at {1}...", C.Localize(trigger.StartTimeUtc), C.Localize(trigger.GetNextFireTimeUtc()));

                sched.ScheduleJob(job, trigger);
            }
            else
                C.Log("Scheduler has not started!");
        }

        #region Start / Stop

        public override bool OnStart()
        {
            var ready = false;

            C.Log("Starting worker role, switching to warning logging only...");
            RoleEnvironment.TraceSource.Switch.Level = SourceLevels.Warning;

            try
            {
                if (base.OnStart())
                {
                    C.Log("Initializing lock...");
                    Lock.Init();

                    C.Log("Writing initial queue targets...");
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
                C.Log("Role is shutting down, winding down...");
                completed.Set();

                C.Log("Stopping scheduler...");
                sched.Shutdown(false);

                C.Log("Stopping role...");
                base.OnStop();

                C.Log("Role has shut down.");
            }
            catch (Exception ex)
            {
                C.Log("Error during shutdown: ", ex);
            }
        }

        #endregion
    }
}
