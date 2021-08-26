using Eol.Cig.Etl.Shared.Configuration;
using System.Configuration;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public class BigQueryJobElement : JobConfigurationElementBase, IConfigurationElement
    {
        [ConfigurationProperty("name", IsKey = true, IsRequired = true)]
        public string Name => base["name"];

        [ConfigurationProperty("sqlCsvDelimiter", IsRequired = false)]
        public string SqlCsvDelimiter => base["sqlCsvDelimiter"];

        [ConfigurationProperty("destinationConnectionStringName", IsRequired = true)]
        public string DestinationConnectionStringName => base["destinationConnectionStringName"];

        [ConfigurationProperty("destinationTable", IsRequired = true)]
        public string DestinationTable => base["destinationTable"];

        [ConfigurationProperty("dataSetId", IsRequired = true)]
        public string DataSetId => base["dataSetId"];

        [ConfigurationProperty("tableName", IsRequired = true)]
        public string TableName => base["tableName"];
    }
}
