using Eol.Cig.Etl.BigQuery.Configuration;
using Eol.Cig.Etl.BigQuery.Jobs;
using Eol.Cig.Etl.BigQuery.Load;
using Eol.Cig.Etl.BigQuery.Transform;
using Eol.Cig.Etl.Shared.Load;
using log4net;
using Xunit;
using Moq;
using System;
using System.Threading.Tasks;

namespace Eol.Cig.Etl.BigQuery.Tests
{
    public class BigQueryExportTests
    {
        private ILog _logger;
        private IBigQueryServiceFactory _bigQueryServiceFactory;
        private IBigQueryConfiguration _bigQueryConfiguration;
        private ISqlServerUploaderFactory _sqlServerUploaderFactory;
        private IAwsConfiguration _awsConfiguration;

        private Mock<IBigQueryServiceFactory> _bigQueryServiceFactoryMock;
        private Mock<IBigQueryConfiguration> _bigQueryConfigurationMock;
        private Mock<ISqlServerUploaderFactory> _sqlServerUploaderFactoryMock;
        private Mock<IAwsConfiguration> _awsConfigurationMock;

        private string _jobName = "TestJob";

        public BigQueryExportTests()
        {
            _logger = LogManager.GetLogger(nameof(BigQueryExportTests));
            _bigQueryConfigurationMock = new Mock<IBigQueryConfiguration>();
            _bigQueryServiceFactoryMock = new Mock<IBigQueryServiceFactory>();
            _sqlServerUploaderFactoryMock = new Mock<ISqlServerUploaderFactory>();
            _awsConfigurationMock = new Mock<IAwsConfiguration>();


            var bigQueryJobConfigurationMock = new Mock<IBigQueryJobConfiguration>();
            _bigQueryConfigurationMock
                .Setup(x => x.GetJobConfiguration(It.IsAny<string>()))
                .Returns(bigQueryJobConfigurationMock.Object);

            var bigQuerySqlServerUploaderMock = new Mock<IBigQuerySqlServerUploader>();
            _sqlServerUploaderFactoryMock
                .Setup(x => x.Create(It.IsAny<string>()))
                .Returns(bigQuerySqlServerUploaderMock.Object);

            _bigQueryConfiguration = _bigQueryConfigurationMock.Object;
            _bigQueryServiceFactory = _bigQueryServiceFactoryMock.Object;
            _sqlServerUploaderFactory = _sqlServerUploaderFactoryMock.Object;
            _awsConfiguration = _awsConfigurationMock.Object;
        }

        [Fact]
        public void BigQueryExport_Given_All_Dependencies_Construction_Works()
        {
            var bigQueryExport = new BigQueryExport(_logger,_awsConfiguration, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
        }

        [Fact]
        public void BigQueryExport_Given_Some_Of_Dependencies_Are_Null_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _awsConfiguration, null, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
            }
            catch (ArgumentNullException ex)
            {
                Assert.NotNull(ex);
            }
        }

        [Fact]
        public void BigQueryExport_Given_JobConfiguration_Is_Not_Present_In_The_Global_Configuration_ArgumentNullException_Is_Thrown()
        {
            try
            {
                _bigQueryConfigurationMock
                    .Setup(x => x.GetJobConfiguration(It.IsAny<string>()))
                    .Returns((IBigQueryJobConfiguration)null);
                _bigQueryConfiguration = _bigQueryConfigurationMock.Object;

                var bigQueryExport = new BigQueryExport(_logger, _awsConfiguration, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
            }
            catch (ArgumentNullException ex)
            {
                Assert.NotNull(ex);
            }
        }

        [Fact]
        public async Task BigQueryExport_When_Export_Is_Called_With_Empty_Query_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _awsConfiguration, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("", DateTime.Now, transformer);
            }
            catch(ArgumentNullException ex)
            {
                Assert.NotNull(ex);
            }
        }

        [Fact]
        public async Task BigQueryExport_When_Export_Is_Called_With_MinDateTime_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _awsConfiguration, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("select a from table", DateTime.MaxValue, transformer);
            }
            catch (ArgumentNullException ex)
            {
                Assert.NotNull(ex);
            }
        }

        [Fact]
        public async Task BigQueryExport_When_Export_Is_Called_With_MaxDateTime_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _awsConfiguration, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("select a from table", DateTime.MaxValue, transformer);
            }
            catch (ArgumentNullException ex)
            {
                Assert.NotNull(ex);
            }
        }
    }
}
