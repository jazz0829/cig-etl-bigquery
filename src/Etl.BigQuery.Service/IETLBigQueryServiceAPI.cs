using Eol.Cig.Etl.Shared.Service;
using System.ServiceModel;
using System.ServiceModel.Web;

namespace Eol.Cig.Etl.BigQuery.Service
{
    [ServiceContract]
    public interface IEtlBigQueryServiceApi : IEtlServiceApi
    {
        [OperationContract(Name = "ExecuteJob")]
        [WebGet]
        string Execute(string jobName);
    }
}
