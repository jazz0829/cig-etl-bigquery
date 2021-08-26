using System.Collections.Generic;

namespace Eol.Cig.Etl.BigQuery.Configuration
{
    public interface IBigQueryConfiguration
    {
        IDictionary<string, IBigQueryJobConfiguration> Jobs { get; }
        string ProjectId { get; }
        string KeyFilePath { get; }
        string KeyFilePassword { get; }
        string Username { get; }

        IBigQueryJobConfiguration GetJobConfiguration(string jobName);
    }
}
