using System.Threading.Tasks;
using Eol.Cig.Etl.Shared.Load;
using log4net;
using Eol.Cig.Etl.BigQuery.Model;
using Eol.Cig.Etl.BigQuery.Configuration;
using System;
using Eol.Cig.Etl.BigQuery.Transform;
using Google.Apis.Bigquery.v2.Data;
using System.Collections.Generic;

namespace Eol.Cig.Etl.BigQuery.Jobs
{
    public class SalesforceExport : BigQueryExport, IEtlJob
    {
        public static readonly string Name = nameof(SalesforceExport).ToUpperInvariant();

        public SalesforceExport(ILog logger,IAwsConfiguration awsConfiguration,
            IBigQueryConfiguration configuration,
            IBigQueryServiceFactory serviceFactory,
            ISqlServerUploaderFactory sqlServerUploaderFactory)
            : base(logger, awsConfiguration, configuration, serviceFactory, sqlServerUploaderFactory, Name)
        {
        }

        public Task Execute()
        {
            var lastExportDate = SqlServerUploader.GetLastExportedDate(JobConfiguration.DataSetId, JobConfiguration.TableName);
            if (lastExportDate == DateTime.MinValue)
            {
                lastExportDate = new DateTime(2017, 5, 29);
            }

            var exportDate = lastExportDate.AddDays(1).Date;
            if (exportDate < DateTime.Now.Date)
            {
                Func<TableCell, object> salesforceIdGenerator = (c) =>
                {
                    var idValue = (string)c.V;
                    if (!string.IsNullOrEmpty(idValue) && idValue.Length == 15)
                    {
                        return SalesforceIdConverter.Convert15CharTo18CharId(idValue);
                    }

                    return idValue;
                };

                var fieldGenerators = new Dictionary<(string, string, Type), Func<TableCell, object>>
                {
                    [("userId", "userIdLong", typeof(string))] = salesforceIdGenerator
                };

                var transformer = new QueryResponseToDataSetTransformer(fieldGenerators);
                var query = SalesforceSessionRecord.Query(JobConfiguration.DataSetId, exportDate);
                return Execute(query, lastExportDate.AddDays(1).Date, transformer);
            }

            Logger.Info($"Export date {exportDate.ToShortDateString()} must before today.");
            return Task.CompletedTask;
        }

        public string GetName()
        {
            return Name;
        }
    }
}
