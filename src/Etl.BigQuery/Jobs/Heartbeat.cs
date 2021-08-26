using log4net;
using System;
using System.Threading.Tasks;

namespace Eol.Cig.Etl.BigQuery.Jobs
{
    public class Heartbeat : IEtlJob
    {
        private readonly ILog _logger;

        public static readonly string Name = nameof(Heartbeat).ToUpperInvariant();

        public Heartbeat(ILog logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public Task Execute()
        {
            _logger.Info("BigQuery service is running.");
            return Task.CompletedTask;
        }

        public string GetName()
        {
            return Name;
        }
    }
}
