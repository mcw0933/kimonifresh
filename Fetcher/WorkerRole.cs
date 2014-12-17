using System;
using System.Net;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Quartz;
using Quartz.Impl;

namespace Fetcher
{
    public class WorkerRole : RoleEntryPoint
    {
        #region Members
        QueueClient queue; // QueueClient is thread-safe.  Recommended to cache rather than recreate.
        ManualResetEvent completed = new ManualResetEvent(false);
        #endregion

        // TO CONFIGURE DIAGNOSTICS:
        // http://azure.microsoft.com/en-us/documentation/articles/cloud-services-dotnet-diagnostics/
        // Right-click the role (under Roles), Properties - Configuration, check Enable Diagnostics, provide
        // a ConnectionString (or use UseDevelopmentStorage=true)

        private void ProcessMessage(BrokeredMessage newMsg)
        {
            C.Log("Processing Service Bus message: " + newMsg.SequenceNumber);

            var schedule = newMsg.GetBody<PollTarget>();

            ScheduleJob(schedule);

            while (true)
            {
                Thread.Sleep(10000);
                C.Log("Working...", "Information");
            }
        }

        #region Job scheduling
        private void ScheduleJob(PollTarget t)
        {
            var websitePingJobDetail = JobBuilder.Create<PollJob>()
                    .WithIdentity("WebsitePingJob", "group1")
                    .WithDescription("Website Ping Job")
                    .UsingJobData(new JobDataMap())
                    .Build();

            var websitePingJobTrigger = TriggerBuilder.Create()
                .WithIdentity("WebsitePingJob", "group1")
                .StartAt(DateBuilder.EvenMinuteDate(C.CurrTime()))
                .WithCronSchedule(RoleEnvironment.GetConfigurationSettingValue("Minutely"))
                .StartNow()
                .Build();

            var sched = new StdSchedulerFactory().GetScheduler();
            sched.Start();
            sched.ScheduleJob(websitePingJobDetail, websitePingJobTrigger);
        }
        #endregion


        #region Worker Role Events / Interface

        public override bool OnStart()
        {
            try
            {
                Lock.Init();

                // Set the maximum number of concurrent connections 
                ServicePointManager.DefaultConnectionLimit = 12;

                // Create the queue if it does not exist already
                var connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
                var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

                var queueName = RoleEnvironment.GetConfigurationSettingValue("PollRequestsQueue");
                if (!namespaceManager.QueueExists(queueName))
                {
                    namespaceManager.CreateQueue(queueName);
                }

                // Initialize the connection to Service Bus Queue
                queue = QueueClient.CreateFromConnectionString(connectionString, queueName);
                return base.OnStart();
            }
            catch (Exception ex)
            {
                C.Log("Error during startup: ", ex);
                return false;
            }
        }

        public override void Run()
        {
            C.Log("Starting processing of messages...");

            // Initiates the message pump,
            // callback is invoked for each message that is received, 
            // calling close on the client will stop the pump.
            queue.OnMessage((newMsg) =>
            {
                try
                {
                    ProcessMessage(newMsg);
                }
                catch (Exception ex)
                {
                    C.Log("Error processing message {0}: ", ex, newMsg.SequenceNumber);
                }
            });



            // TODO: REMOVE after Testing
            ScheduleJob(null);



            completed.WaitOne();
        }

        public override void OnStop()
        {
            try
            {
                // Close the connection to Service Bus Queue
                queue.Close();
                completed.Set();
                base.OnStop();
            }
            catch (Exception ex)
            {
                C.Log("Error during shutdown: ", ex);
            }
        }

        #endregion

        #region Setup
        private void Init()
        {
            
        }
        #endregion
    }
}
