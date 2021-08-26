using Eol.Cig.Etl.Shared.Configuration;
using Eol.Cig.Etl.Shared.Extensions;
using System.Collections.Specialized;

namespace Eol.Cig.Etl.BigQuery.Service.Configuration
{
    public class BigQueryServiceConfiguration : ServiceConfiguration, IBigQueryServiceConfiguration
    {
        public BigQueryServiceConfiguration(NameValueCollection appSettings)
            :base(appSettings)
        {
            Schedule = appSettings.GetStringOrThrow("SCHEDULE");
        }
        public string Schedule { get; }
    }
}
