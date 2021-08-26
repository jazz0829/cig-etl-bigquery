using Eol.Cig.Etl.Shared.Load;
using System;
using System.Data;

namespace Eol.Cig.Etl.BigQuery.Load
{
    public interface IBigQuerySqlServerUploader: ISqlServerUploader
    {
        void Upload(DataTable data, string dataSetId, string tableName, DateTime exportDate);
        DateTime GetLastExportedDate(string dataSetId, string tableName);
    }
}