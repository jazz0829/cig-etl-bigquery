using Eol.Cig.Etl.Shared.Service;
using Eol.Cig.Etl.BigQuery.Service.Configuration;
using log4net;
using Quartz;
using System;

namespace Eol.Cig.Etl.BigQuery.Service
{
    public class EtlBigQueryServiceApi : EtlServiceApi, IEtlBigQueryServiceApi
    {
        private readonly ILog _logger;
        private readonly IScheduler _scheduler;

        public EtlBigQueryServiceApi(
            ILog logger,
            IScheduler scheduler,
            IBigQueryServiceConfiguration configuration
        ): base(configuration)
        {
            _logger = logger;
            _scheduler = scheduler;
        }

        public override string Execute()
        {
            _logger.Warn("Execute called over API, but is not implemented!");
            return "Not implemented!";
        }

        //Contract name: ExecuteJob
        public string Execute(string jobName)
        {
            var normalizedJobName = jobName.ToUpperInvariant();
            var jobKey = new JobKey(normalizedJobName);
            var triggerKey = new TriggerKey($"OnDemand_{jobName}_{DateTime.Now.ToString("yyyyMMddHHmmss")}");

            ITrigger trigger = TriggerBuilder.Create()
                .WithDescription($"On demand execution job: {normalizedJobName}")
                .WithIdentity(triggerKey)
                .ForJob(jobKey)
                .StartNow()
                .Build();

            try
            {
                _scheduler.ScheduleJob(trigger);
                _logger.Info($"Executed {normalizedJobName} on demand.");

            }
            catch (Exception ex)
            {
                _logger.Error($"Error occured during on deman execution of job: {normalizedJobName}.", ex);
            }
            finally
            {
                _scheduler.UnscheduleJob(triggerKey);
            }

            return $"Executed {normalizedJobName}.";
        }
    }
}
