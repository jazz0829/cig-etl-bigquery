using Eol.Cig.Etl.BigQuery.Service.Configuration;
using StructureMap;
using Quartz;
using Topshelf;
using Topshelf.Quartz.StructureMap;
using Topshelf.StructureMap;
using Eol.Cig.Etl.Shared.Service;
using Eol.Cig.Etl.BigQuery.Jobs;
using Eol.Cig.Etl.Shared.Extensions;

namespace Eol.Cig.Etl.BigQuery.Service
{
    static class Program
    {
        static void Main(string[] args)
        {
            HostFactory.Run(hostConfig =>
            {
                Container container = Bootstrapper.CreateContainer();
                var configuration = container.GetInstance<IBigQueryServiceConfiguration>();
                hostConfig.ConfigureHost(configuration);
                hostConfig.UseStructureMap(container);

                hostConfig.Service<IEtlService>(s =>
                {
                    s.ConfigureService();

                    var salesforceExportJobDetails = JobBuilder.Create<QuartzEtlJob<SalesforceExport>>().WithIdentity(SalesforceExport.Name).Build();
                    s.ScheduleQuartzJob(q => q.ConfigureJobWithSchedule(salesforceExportJobDetails, configuration.Schedule));

                    var heartbeatJobDetails = JobBuilder.Create<QuartzEtlJob<Heartbeat>>().WithIdentity(Heartbeat.Name).Build();
                    s.ScheduleQuartzJob(q => q.ConfigureJobWithSchedule(heartbeatJobDetails, configuration.Schedule));
                });
            });
        }
    }
}
