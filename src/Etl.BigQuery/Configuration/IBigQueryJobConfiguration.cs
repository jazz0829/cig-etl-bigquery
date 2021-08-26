using Eol.Cig.Etl.Shared.Configuration;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public interface IBigQueryJobConfiguration: ISqlJobConfiguration
    {
        string DataSetId { get; }
        string TableName { get; }
    }
}
