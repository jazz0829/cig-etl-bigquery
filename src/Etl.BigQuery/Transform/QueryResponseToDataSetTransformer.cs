using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Data;

namespace Eol.Cig.Etl.BigQuery.Transform
{
    public class QueryResponseToDataSetTransformer
    {
        private static readonly string ETL_INSERT_TIME = "EtlInsertTime";

        private readonly IDictionary<string, Func<TableCell, object>> _fieldTransformers;
        private readonly IDictionary<(string source, string destination, Type destinationType), Func<TableCell, object>> _fieldGenerators;
        private readonly Func<TableCell, object> _defaultTransformer = (c) => c.V;

        public QueryResponseToDataSetTransformer()
            : this(
                  new Dictionary<string, Func<TableCell, object>>(),
                  new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>())
        { }

        public QueryResponseToDataSetTransformer(IDictionary<string, Func<TableCell, object>> fieldTransformers)
            : this(
                 fieldTransformers,
                 new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>())
        { }

        public QueryResponseToDataSetTransformer(IDictionary<(string source, string destination, Type destinationType), Func<TableCell, object>> fieldGenerators)
           : this(
                new Dictionary<string, Func<TableCell, object>>(),
                fieldGenerators)
        { }

        public QueryResponseToDataSetTransformer(
            IDictionary<string, Func<TableCell, object>> fieldTransformers,
            IDictionary<(string source, string destination, Type destinationType), Func<TableCell, object>> fieldGenerators)
        {
            _fieldTransformers = fieldTransformers ?? throw new ArgumentNullException(nameof(fieldTransformers));
            _fieldGenerators = fieldGenerators ?? throw new ArgumentNullException(nameof(fieldGenerators));
        }

        private Func<TableCell, object> GetFieldTransformer(string fieldName)
        {
            return _fieldTransformers.ContainsKey(fieldName) ? _fieldTransformers[fieldName] : _defaultTransformer;
        }

        public DataSet Transform(TableSchema schema, IList<TableRow> rows, DateTime etlInsertTime)
        {
            var resultSet = new DataSet();
            var resultTable = new DataTable();
            var columnIndexMap = new Dictionary<string, int>();

            //create dataset schema based on query response schema
            for (var i = 0; i < schema.Fields.Count; i++)
            {
                var field = schema.Fields[i];
                var column = new DataColumn(field.Name, GetDotNetType(field.Type))
                {
                    AllowDBNull = true
                };
                resultTable.Columns.Add(column);
                columnIndexMap.Add(field.Name, i);
            }

            //Add additional generated fields
            foreach (var generatedField in _fieldGenerators.Keys)
            {
                var column = new DataColumn(generatedField.destination, generatedField.destinationType)
                {
                    AllowDBNull = true
                };
                resultTable.Columns.Add(column);
            }

            //Add etlInsertTime
            resultTable.Columns.Add(new DataColumn(ETL_INSERT_TIME, typeof(DateTime)));

            foreach (var row in rows)
            {
                var dataTableRow = resultTable.NewRow();
                for (var i = 0; i < row.F.Count; i++)
                {
                    var fieldName = schema.Fields[i].Name;
                    var transformer = GetFieldTransformer(fieldName);
                    var field = row.F[i];
                    var transformedValue = transformer(field);
                    dataTableRow[i] = transformedValue ?? DBNull.Value;
                }

                foreach (var generator in _fieldGenerators)
                {
                    var sourceField = generator.Key.source;
                    var destinationField = generator.Key.destination;
                    var generatorFunc = generator.Value;
                    if (!columnIndexMap.TryGetValue(sourceField, out var sourceFieldIndex))
                    {
                        throw new Exception($"Source Google Analytics Field does not exist! Field name: {sourceField}");
                    }

                    var sourceValue = row.F[sourceFieldIndex];
                    var generatedValue = generatorFunc(sourceValue);

                    dataTableRow[destinationField] = generatedValue ?? DBNull.Value;
                }

                dataTableRow[ETL_INSERT_TIME] = etlInsertTime;

                resultTable.Rows.Add(dataTableRow);
            }

            resultSet.Tables.Add(resultTable);
            return resultSet;
        }

        // Summary:
        //     [Required] The field data type. Possible values include STRING, BYTES, INTEGER,
        //     INT64 (same as INTEGER), FLOAT, FLOAT64 (same as FLOAT), BOOLEAN, BOOL (same
        //     as BOOLEAN), TIMESTAMP, DATE, TIME, DATETIME, RECORD (where RECORD indicates
        //     that the field contains a nested schema) or STRUCT (same as RECORD).

        private static Type GetDotNetType(string type)
        {
            switch (type)
            {
                case "STRING":
                case "TIME":
                case "DATE":
                    return typeof(string);
                case "BYTES":
                    return typeof(byte[]);
                case "INTEGER":
                case "INT64":
                case "TIMESTAMP":
                    return typeof(long);
                case "FLOAT":
                case "FLOAT64":
                    return typeof(float);
                case "BOOLEAN":
                case "BOOL":
                    return typeof(bool);
                case "DATETIME":
                    return typeof(DateTime);
                default:
                    return typeof(object);
            }
        }
    }
}
