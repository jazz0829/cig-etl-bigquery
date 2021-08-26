using System.Collections.Generic;
using System.Configuration;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public class BigQueryConfiguration : IBigQueryConfiguration
    {
        public static readonly string SectionName = "bigQueryConfiguration";

        public BigQueryConfiguration(BigQueryConfigurationSection config, ConnectionStringSettingsCollection connectionStrings)
        {
            Username = config.Username;
            KeyFilePath = config.KeyFilePath;
            KeyFilePassword = config.KeyFilePassword;
            ProjectId = config.ProjectId;

            Jobs = new Dictionary<string, IBigQueryJobConfiguration>();

            foreach (var jobConfigObject in config.Instances)
            {
                if (jobConfigObject is BigQueryJobElement jobConfig)
                {
                    var job = new BigQueryJobConfiguration(jobConfig, connectionStrings);
                    Jobs.Add(job.Name.ToUpperInvariant(), job);
                }
            }
        }

        public IDictionary<string, IBigQueryJobConfiguration> Jobs { get; private set; }

        public string ProjectId { get; private set; }

        public string KeyFilePath { get; private set; }
        public string KeyFilePassword { get; private set; }

        public string Username { get; private set; }

        public IBigQueryJobConfiguration GetJobConfiguration(string jobName)
        {
            Jobs.TryGetValue(jobName.ToUpperInvariant(), out var jobConfig);
            return jobConfig;
        }
    }
}
