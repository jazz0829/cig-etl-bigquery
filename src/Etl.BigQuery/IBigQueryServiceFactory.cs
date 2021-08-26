using Google.Apis.Bigquery.v2;

namespace Eol.Cig.Etl.BigQuery
{
    public interface IBigQueryServiceFactory
    {
        BigqueryService GetAuthorizedService();
    }
}