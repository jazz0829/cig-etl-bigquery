using Eol.Cig.Etl.BigQuery.Configuration;
using Eol.Cig.Etl.BigQuery.Jobs;
using Eol.Cig.Etl.BigQuery.Load;
using Eol.Cig.Etl.BigQuery.Transform;
using Eol.Cig.Etl.Shared.Load;
using log4net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Threading.Tasks;

namespace Eol.Cig.Etl.BigQuery.Tests
{
    [TestClass]
    public class BigQueryExportTests
    {
        private ILog _logger;
        private IBigQueryServiceFactory _bigQueryServiceFactory;
        private IBigQueryConfiguration _bigQueryConfiguration;
        private ISqlServerUploaderFactory _sqlServerUploaderFactory;

        private Mock<IBigQueryServiceFactory> _bigQueryServiceFactoryMock;
        private Mock<IBigQueryConfiguration> _bigQueryConfigurationMock;
        private Mock<ISqlServerUploaderFactory> _sqlServerUploaderFactoryMock;

        private string _jobName = "TestJob";

        [TestInitialize]
        public void Setup()
        {
            _logger = LogManager.GetLogger(nameof(BigQueryExportTests));
            _bigQueryConfigurationMock = new Mock<IBigQueryConfiguration>();
            _bigQueryServiceFactoryMock = new Mock<IBigQueryServiceFactory>();
            _sqlServerUploaderFactoryMock = new Mock<ISqlServerUploaderFactory>();


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
        }

        [TestMethod]
        public void BigQueryExport_Given_All_Dependencies_Construction_Works()
        {
            var bigQueryExport = new BigQueryExport(_logger, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
        }

        [TestMethod]
        public void BigQueryExport_Given_Some_Of_Dependencies_Are_Null_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, null, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
            }
            catch (ArgumentNullException ex)
            {
                Assert.IsNotNull(ex);
            }
        }

        [TestMethod]
        public void BigQueryExport_Given_JobConfiguration_Is_Not_Present_In_The_Global_Configuration_ArgumentNullException_Is_Thrown()
        {
            try
            {
                _bigQueryConfigurationMock
                    .Setup(x => x.GetJobConfiguration(It.IsAny<string>()))
                    .Returns((IBigQueryJobConfiguration)null);
                _bigQueryConfiguration = _bigQueryConfigurationMock.Object;

                var bigQueryExport = new BigQueryExport(_logger, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
            }
            catch (ArgumentNullException ex)
            {
                Assert.IsNotNull(ex);
            }
        }

        [TestMethod]
        public async Task BigQueryExport_When_Export_Is_Called_With_Empty_Query_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("", DateTime.Now, transformer);
            }
            catch(ArgumentNullException ex)
            {
                Assert.IsNotNull(ex);
            }
        }

        [TestMethod]
        public async Task BigQueryExport_When_Export_Is_Called_With_MinDateTime_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("select a from table", DateTime.MaxValue, transformer);
            }
            catch (ArgumentNullException ex)
            {
                Assert.IsNotNull(ex);
            }
        }

        [TestMethod]
        public async Task BigQueryExport_When_Export_Is_Called_With_MaxDateTime_ArgumentNullException_Is_Thrown()
        {
            try
            {
                var bigQueryExport = new BigQueryExport(_logger, _bigQueryConfiguration, _bigQueryServiceFactory, _sqlServerUploaderFactory, _jobName);
                var transformer = new QueryResponseToDataSetTransformer();
                await bigQueryExport.Execute("select a from table", DateTime.MaxValue, transformer);
            }
            catch (ArgumentNullException ex)
            {
                Assert.IsNotNull(ex);
            }
        }
    }
}
