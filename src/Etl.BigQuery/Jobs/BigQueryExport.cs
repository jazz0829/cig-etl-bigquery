using Amazon.Kinesis;
using Eol.Cig.Etl.BigQuery.Configuration;
using Eol.Cig.Etl.BigQuery.Load;
using Eol.Cig.Etl.BigQuery.Transform;
using Eol.Cig.Etl.Kinesis.Producer;
using Eol.Cig.Etl.Shared.Load;
using Google;
using Google.Apis.Bigquery.v2.Data;
using log4net;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Eol.Cig.Etl.BigQuery.Jobs
{
    public class BigQueryExport
    {
        protected readonly ILog Logger;
        private readonly IBigQueryServiceFactory _bigQueryServiceFactory;
        private readonly IBigQueryConfiguration _configuration;
        protected readonly IBigQueryJobConfiguration JobConfiguration;
        protected readonly IBigQuerySqlServerUploader SqlServerUploader;
        private readonly string _jobName;
        private readonly IAmazonKinesis _kinesis;
        protected readonly IAwsConfiguration _awsConfiguration;

        public BigQueryExport(ILog logger, IAwsConfiguration awsConfiguration,
            IBigQueryConfiguration configuration,
            IBigQueryServiceFactory serviceFactory,
            ISqlServerUploaderFactory sqlServerUploaderFactory,
            string jobName
           )
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _bigQueryServiceFactory = serviceFactory ?? throw new ArgumentNullException(nameof(serviceFactory));
            var sqlServerUploaderFactory1 = sqlServerUploaderFactory ?? throw new ArgumentNullException(nameof(sqlServerUploaderFactory));
            _jobName = string.IsNullOrEmpty(jobName) ? throw new ArgumentNullException(nameof(jobName)) : jobName;

            _awsConfiguration = awsConfiguration;
            _kinesis = new AmazonKinesisClient(awsConfiguration.AwsAccessKeyId, awsConfiguration.AwsSecretAccessKey, Amazon.RegionEndpoint.EUWest1);

            JobConfiguration = _configuration.GetJobConfiguration(_jobName);
            SqlServerUploader = sqlServerUploaderFactory1.Create(_jobName) as IBigQuerySqlServerUploader;

            if (JobConfiguration == null)
            {
                throw new ArgumentNullException($"Unable to find job configuration for {_jobName}");
            }

            if (SqlServerUploader == null)
            {
                throw new ArgumentNullException($"Unable to create BigQuerySqlServerUploader for {_jobName}");
            }
        }

        public async Task Execute(string query, DateTime exportDate, QueryResponseToDataSetTransformer transformer)
        {
            // Check input arguments are valid
            if (string.IsNullOrEmpty(query))
            {
                throw new ArgumentNullException(nameof(query));
            }

            if (exportDate == DateTime.MaxValue || exportDate == DateTime.MinValue)
            {
                throw new ArgumentNullException(nameof(exportDate));
            }

            if (transformer == null)
            {
                throw new ArgumentNullException(nameof(transformer));
            }

            Logger.Info($"JobName: {_jobName}. Executing BigQuery export for {exportDate.ToShortDateString()}.");

            var bigQueryService = _bigQueryServiceFactory.GetAuthorizedService();
            var queryRequest = new QueryRequest { Query = query, UseLegacySql = false, MaxResults = 1000 };

            QueryResponse response = null;

            try
            {
                response = await bigQueryService.Jobs.Query(queryRequest, _configuration.ProjectId).ExecuteAsync();
            }
            catch (GoogleApiException ex)
            {
                if (ex.Error.Code == 404)
                {
                    Logger.Warn(ex.Error.Message);
                }
                else
                {
                    throw;
                }
            }
            if (response == null)
            {
                Logger.Warn($"The response for date: {exportDate.ToShortDateString()} is null");
                return;
            }

            if (response.Errors?.Count > 0)
            {
                foreach (var error in response.Errors)
                {
                    Logger.ErrorFormat("BigQuery error. Reason: {0}\tMessage: {1}\tLocation: {2}", error.Reason, error.Message, error.Location);
                }

                throw new Exception($"Error occured when querying BigQuery for date: {exportDate.ToShortDateString()}.");
            }

            var totalRows = response.TotalRows.GetValueOrDefault();

            if (totalRows == 0)
            {
                Logger.Warn($"There were no rows in the response for date: {exportDate.ToShortDateString()}");
                return;
            }

            var totalRowsExported = (ulong)response.Rows.Count;
            var pageToken = response.PageToken;
            var jobId = response.JobReference.JobId;
            var schema = response.Schema;

            Logger.Info($"JobName: {_jobName}. Exported first batch. JobId: {jobId}. Total rows to export: {totalRows}");

            var data = new List<TableRow>((int)totalRows);
            data.AddRange(response.Rows);

            var initialDataSet = transformer.Transform(schema, data, DateTime.Now);

            //Load raw response into destination
            Logger.Info($"JobName: {_jobName}. Uploading data to SQL Server.");
            SqlServerUploader.Upload(initialDataSet.Tables[0], JobConfiguration.DataSetId, JobConfiguration.TableName, exportDate);

            if (_awsConfiguration.IsStreamingEnabled)
            {
                var kinesisProducer = new KinesisWriter(Logger, _kinesis, _awsConfiguration.AwsKinesisStreamName, _awsConfiguration.S3Prefix, _jobName.Replace("Export", ""));

                kinesisProducer.IngestData(initialDataSet.Tables[0]);
            }

            while (pageToken != null)
            {
                var batchData = new List<TableRow>();
                var pageRequest = bigQueryService.Jobs.GetQueryResults(_configuration.ProjectId, jobId);
                pageRequest.PageToken = pageToken;
                var pageResponse = await pageRequest.ExecuteAsync();

                batchData.AddRange(pageResponse.Rows);
                totalRowsExported += (ulong)pageResponse.Rows.Count;
                pageToken = pageResponse.PageToken;

                var dataSet = transformer.Transform(schema, batchData, DateTime.Now);

                //Load raw response into destination
                Logger.Info($"JobName: {_jobName}. Uploading data to SQL Server.");
                SqlServerUploader.Upload(dataSet.Tables[0], JobConfiguration.DataSetId, JobConfiguration.TableName, exportDate);


                Logger.Info($"JobName: {_jobName}. Exported batch. JobId: {jobId}. Rows exported: {totalRowsExported}. Total rows to export: {totalRows}");

                if (_awsConfiguration.IsStreamingEnabled)
                {
                    var kinesisProducer = new KinesisWriter(Logger, _kinesis, _awsConfiguration.AwsKinesisStreamName, _awsConfiguration.S3Prefix, _jobName.Replace("Export", ""));

                    kinesisProducer.IngestData(dataSet.Tables[0]);
                }
            }

            Logger.Info($"JobName: {_jobName}. Export done for {exportDate.ToShortDateString()}.");
        }
    }
}
