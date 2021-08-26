using System.Configuration;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public class BigQueryConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty("username", IsKey = true, IsRequired = true)]
        public string Username => (string)this["username"];

        [ConfigurationProperty("keyFilePath", IsKey = true, IsRequired = true)]
        public string KeyFilePath => (string)this["keyFilePath"];

        [ConfigurationProperty("keyFilePassword", IsKey = true, IsRequired = true)]
        public string KeyFilePassword => (string)this["keyFilePassword"];

        [ConfigurationProperty("projectId", IsKey = true, IsRequired = true)]
        public string ProjectId => (string)this["projectId"];

        [ConfigurationProperty("", IsRequired = true, IsDefaultCollection = true)]
        public BigQueryJobConfigurationCollection Instances
        {
            get => (BigQueryJobConfigurationCollection)this[""];
            set => this[""] = value;
        }
    }
}
