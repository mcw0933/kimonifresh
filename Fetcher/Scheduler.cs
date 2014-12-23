using System;
using Quartz;

namespace Fetcher
{
    class Time
    {
        internal static DateTimeOffset NextRunFor(Schedule sched)
        {
            var s = sched.ToString();
            var now = C.CurrTime();

            switch (s)
            {
                case Schedule.HOURLY:
                    return DateBuilder.FutureDate(1, IntervalUnit.Hour);

                case Schedule.WEEKDAILY:
                    switch (now.DayOfWeek)
                    {
                        case DayOfWeek.Friday:
                            return Days(3);

                        case DayOfWeek.Saturday:
                            return Days(2);

                        default:
                            return Days(1);
                    }

                case Schedule.MWF:
                    switch (now.DayOfWeek)
                    {
                        case DayOfWeek.Friday:
                            return Days(3);

                        case DayOfWeek.Monday:
                        case DayOfWeek.Wednesday:
                        case DayOfWeek.Saturday:
                            return Days(2);

                        default:
                            return Days(1);
                    }

                case Schedule.DAILY:
                    return Days(1);

                case Schedule.WEEKLY:
                    return Days(7);

                default:
                    if (CronExpression.IsValidExpression(s))
                    {
                        var next = new CronExpression(s).GetNextValidTimeAfter(now);
                        if (next.HasValue)
                            return next.Value;
                    }

                    return Days(1);
            }
        }

        public static DateTimeOffset Days(int days)
        {
            return DateBuilder.FutureDate(days, IntervalUnit.Day);
        }

        public static DateTimeOffset Mins(int mins)
        {
            return DateBuilder.FutureDate(mins, IntervalUnit.Minute);
        }
    }

    internal class Schedule
    {
        public const string HOURLY = "Hourly";
        public const string DAILY = "Daily";
        public const string MWF = "MWF";
        public const string WEEKDAILY = "Weekdaily";
        public const string WEEKLY = "Weekly";
        

        private string sched = HOURLY;

        public static implicit operator Schedule(string s)
        {
            return new Schedule() { sched = s };
        }

        public override string ToString()
        {
            return sched;
        }

        internal static void For(DateTimeOffset nextCheck, IJobExecutionContext context)
        {
            var key = context.Trigger.Key;
            var trigger = context.Trigger.GetTriggerBuilder().StartAt(nextCheck).Build();

            context.Scheduler.RescheduleJob(key, trigger);
        }
    }
}
