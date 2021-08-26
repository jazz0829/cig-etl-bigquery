using System;
using log4net;
using Quartz;

namespace Eol.Cig.Etl.BigQuery.Service
{
    [DisallowConcurrentExecution]
    public class QuartzEtlJob<T> : IJob where T : IEtlJob
    {
        private readonly ILog _logger;
        private readonly T _job;
        public QuartzEtlJob(ILog logger, T eloquaJob)
        {
            _logger = logger;
            _job = eloquaJob;
        }

        public void Execute(IJobExecutionContext context)
        {
            var jobName = context.JobDetail.Key.Name;
            var groupName = context.JobDetail.Key.Group;
            ThreadContext.Properties["JobName"] = jobName;
            ThreadContext.Properties["JobGroupName"] = groupName;
            _logger.InfoFormat("Job {0} from group {1} executing...", jobName, groupName);
            _logger.InfoFormat("Triggered by: {0}", context.Trigger.Description);

            try
            {
                var jobTask = _job.Execute();
                jobTask.Wait();

                _logger.Info($"Job {jobName} finished successfully!");
            }
            catch (Exception ex)
            {
                _logger.Fatal($"Error occured while executing job {jobName}.", ex);
            }
        }
    }
}
