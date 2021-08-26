using Eol.Cig.Etl.Shared.Service;
using Eol.Cig.Etl.Shared.Api;
using log4net;
using Eol.Cig.Etl.BigQuery.Service.Configuration;

namespace Eol.Cig.Etl.BigQuery.Service
{
    public class EtlBigQueryService : EtlService<IEtlBigQueryServiceApi, EtlBigQueryServiceApi>
    {
        private readonly ILog _logger;
        private readonly IBigQueryServiceConfiguration _configuration;

        public EtlBigQueryService(IBigQueryServiceConfiguration configuration, ILog logger, IHostedApiFactory hostedApiFactory)
            : base(configuration, logger, hostedApiFactory)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override void LogSchedule()
        {
            _logger.InfoFormat("Job {0} Starts at {1}", "BigQueryJobs", _configuration.Schedule);
        }
    }
}
