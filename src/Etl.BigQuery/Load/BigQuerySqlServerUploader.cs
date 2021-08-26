using Eol.Cig.Etl.Shared.Load;
using System;
using Eol.Cig.Etl.Shared.Configuration;
using log4net;
using System.Data;
using Eol.Cig.Etl.Shared.Utils;
using System.Data.SqlClient;
using System.Linq;

namespace Eol.Cig.Etl.BigQuery.Load
{
    public class BigQuerySqlServerUploader : SqlServerUploader, IBigQuerySqlServerUploader
    {
        public BigQuerySqlServerUploader(ILog logger, ISqlJobConfiguration configuration)
            : base(logger, configuration)
        {
        }

        public DateTime GetLastExportedDate(string dataSetId, string tableName)
        {
            var lastExportedDate = DateTime.MinValue;

            var sqlQuery =
                $@"SELECT MAX(ExportDate) as ExportDate
                   FROM config.BigQuery_DataExportLog
                   WHERE DataSetId = '{dataSetId}' AND TableName = '{tableName}'";

            using (var reader = SqlServerUtils.ExecuteCommandReturnReader(sqlQuery, Configuration.DestinationConnectionString))
            {
                if (reader != null && reader.HasRows)
                {
                    reader.Read();
                    lastExportedDate = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0);
                    Logger.Info($"LastExportDate for data set {dataSetId} and table {tableName} was {lastExportedDate.ToShortDateString()}");
                }
                else
                {
                    Logger.Info($"There were no records in the export log for dataSetId: {dataSetId} and tableName: {tableName}");
                }
            }

            return lastExportedDate;
        }

        public void Upload(DataTable data, string dataSetId, string tableName, DateTime exportDate)
        {
            //int max = 0;
            //foreach(DataRow item in data.Rows)
            //{
            //    if(item[121] != null)
            //    {
            //        if(item[121].ToString().Length > max)
            //        {
            //            max = item[121].ToString().Length;
            //        }
            //        Console.WriteLine(item[121]);
            //    }
            //}


            using (var conn = Retry.Do(() => SqlServerUtils.OpenConnection(Configuration.DestinationConnectionString), TimeSpan.FromSeconds(60), 5))
            {
                using (var tran = conn.BeginTransaction())
                {
                    BulkInsert(data, conn, tran, Configuration.DestinationTable);
                    SaveLastExportDate(dataSetId, tableName, exportDate, conn, tran);
                    tran.Commit();
                }
            }
        }

        private void SaveLastExportDate(string dataSetId, string tableName, DateTime exportDate, SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            var updateQuery = @"INSERT INTO config.BigQuery_DataExportLog
                                 (DataSetId, TableName, ExportDate, InsertTime)
                                 VALUES(@DataSetId,@TableName,@ExportDate,@InsertTime)";

            var dataSetIdParam = new SqlParameter("DataSetId", SqlDbType.NVarChar);
            var tableNameParam = new SqlParameter("TableName", SqlDbType.NVarChar);
            var exportDateParam = new SqlParameter("ExportDate", SqlDbType.DateTime);
            var insertTimeParam = new SqlParameter("InsertTime", SqlDbType.DateTime);

            dataSetIdParam.Value = dataSetId;
            tableNameParam.Value = tableName;
            exportDateParam.Value = exportDate;
            insertTimeParam.Value = DateTime.Now;

            SqlServerUtils.ExecuteCommandReturnNone(updateQuery, sqlConnection, sqlTransaction, dataSetIdParam, tableNameParam, exportDateParam, insertTimeParam);
        }
    }
}
