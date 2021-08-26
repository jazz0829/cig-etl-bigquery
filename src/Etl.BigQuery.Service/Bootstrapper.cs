using Eol.Cig.Etl.BigQuery.Configuration;
using Eol.Cig.Etl.BigQuery.Jobs;
using Eol.Cig.Etl.BigQuery.Load;
using Eol.Cig.Etl.BigQuery.Service.Configuration;
using Eol.Cig.Etl.Shared.Configuration;
using Eol.Cig.Etl.Shared.Load;
using Eol.Cig.Etl.Shared.Service;
using Eol.Cig.Etl.Shared.Utils;
using log4net;
using StructureMap;
using System.Configuration;

namespace Eol.Cig.Etl.BigQuery.Service
{
    public static class Bootstrapper
    {
        static IBigQueryServiceConfiguration ServiceConfiguration = new BigQueryServiceConfiguration(ConfigurationManager.AppSettings);
        static IBigQueryConfiguration Configuration = new BigQueryConfiguration(ConfigurationManager.GetSection(BigQueryConfiguration.SectionName) 
            as BigQueryConfigurationSection, ConfigurationManager.ConnectionStrings);
        static readonly IAwsConfiguration awsConfig = new AwsConfiguration();

        public static Container CreateContainer()
        {
            var parentContainer = BootstrapperUtils.CreateDefaultContainer(ServiceConfiguration);

            parentContainer.Configure(cfg =>
            {
                cfg.For<IBigQueryServiceConfiguration>().Use(ServiceConfiguration).Singleton();
                cfg.For<IServiceConfiguration>().Use(ServiceConfiguration).Singleton();
                cfg.For<IBigQueryConfiguration>().Use(Configuration).Singleton();
                cfg.For<IEtlService>().Use<EtlBigQueryService>().Singleton();
                cfg.For<IEtlBigQueryServiceApi>().Use<EtlBigQueryServiceApi>().Singleton();
                cfg.For<ISqlServerUploaderFactory>().Use<SqlServerUploaderFactory>();
                cfg.For<IAwsConfiguration>().Use(awsConfig).Singleton();

                cfg.For<IBigQueryServiceFactory>()
                    .Use(f => new BigQueryServiceFactory(f.GetInstance<ILog>(), Configuration.Username, Configuration.KeyFilePath, Configuration.KeyFilePassword)).Singleton();
                cfg.For<ISqlServerUploaderFactory>().Use<SqlServerUploaderFactory>();

                foreach (var jobConfig in Configuration.Jobs)
                {
                    cfg.For<ISqlJobConfiguration>().Add(jobConfig.Value).Named(jobConfig.Key);
                    cfg.For<ISqlServerUploader>()
                        .Add<BigQuerySqlServerUploader>()
                        .Named(jobConfig.Key)
                        .Ctor<ISqlJobConfiguration>()
                        .IsNamedInstance(jobConfig.Key);
                }

                cfg.For<IEtlJob>().AddInstances(i =>
                {
                    i.Type<SalesforceExport>().Named(SalesforceExport.Name);
                    i.Type<Heartbeat>().Named(Heartbeat.Name);
                });

            });


            return parentContainer;
        }

    }
}
