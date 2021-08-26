using System.Threading.Tasks;

namespace Eol.Cig.Etl.BigQuery
{
    public interface IEtlJob
    {
        string GetName();
        Task Execute();
    }
}
