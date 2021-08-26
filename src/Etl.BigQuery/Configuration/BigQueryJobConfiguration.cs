using Eol.Cig.Etl.Shared.Extensions;
using System.Configuration;
using System.Data.SqlClient;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public class BigQueryJobConfiguration : IBigQueryJobConfiguration
    {
        private readonly SqlConnectionStringBuilder _builder;

        public BigQueryJobConfiguration(BigQueryJobElement jobSettings,
            ConnectionStringSettingsCollection connectionStringSettings)
        {
            Name = jobSettings.GetStringOrThrow("name");

            DestinationConnectionStringName = jobSettings.GetStringOrThrow("destinationConnectionStringName");
            DestinationConnectionString = connectionStringSettings.GetConnectionStringOrThrow(DestinationConnectionStringName);
            DestinationTable = jobSettings.GetStringOrThrow("destinationTable");
            DataSetId = jobSettings.GetStringOrThrow("dataSetId");
            TableName = jobSettings.GetStringOrThrow("tableName");

            _builder = new SqlConnectionStringBuilder(DestinationConnectionString);
        }

        public string DataSetId { get; }
        public string TableName { get; }

        public char SqlCsvDelimiter => ',';
        public string DestinationConnectionStringName { get; }
        public string DestinationConnectionString { get; set; }
        public string Name { get; }
        public string ServerName => _builder.DataSource;
        public string DatabaseName => _builder.InitialCatalog;
        public string DestinationTable { get; }
        public string SalesforceObject { get; }
    }
}
