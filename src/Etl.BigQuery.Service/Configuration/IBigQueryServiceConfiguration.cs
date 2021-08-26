using Eol.Cig.Etl.Shared.Configuration;

namespace Eol.Cig.Etl.BigQuery.Service.Configuration
{
    public interface IBigQueryServiceConfiguration: IServiceConfiguration
    {
        string Schedule { get; }
    }
}
