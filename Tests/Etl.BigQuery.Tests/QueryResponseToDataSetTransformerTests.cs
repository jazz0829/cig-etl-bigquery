using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Eol.Cig.Etl.BigQuery.Transform;
using System.Collections.Generic;
using Google.Apis.Bigquery.v2.Data;
using System.Data;

namespace Etl.BigQuery.Tests
{
    [TestClass]
    public class QueryResponseToDataSetTransformerTests
    {


        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_No_Parameters_Construction_Works()
        {
            var transformer = new QueryResponseToDataSetTransformer();
            Assert.IsNotNull(transformer);
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Only_FieldTransformers_Construction_Works()
        {
            var fieldTransformers = new Dictionary<string, Func<TableCell, object>>();
            var transformer = new QueryResponseToDataSetTransformer(fieldTransformers);
            Assert.IsNotNull(transformer);
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Null_FieldTransformers_Construction_Throws_ArgumentNullException()
        {
            try
            {
                var transformer = new QueryResponseToDataSetTransformer((IDictionary<string, Func<TableCell, object>>)null);
            }
            catch (ArgumentNullException ex)
            {
                Assert.IsNotNull(ex, "Expected exception");
            }
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Only_FieldGenerators_Construction_Works()
        {
            var fieldGenerators = new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>();
            var transformer = new QueryResponseToDataSetTransformer(fieldGenerators);
            Assert.IsNotNull(transformer);
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Null_FieldGenerators_Construction_Throws_ArgumentNullException()
        {
            try
            {
                var transformer = new QueryResponseToDataSetTransformer((IDictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>)null);
            }
            catch(ArgumentNullException ex)
            {
                Assert.IsNotNull(ex, "Expected exception");
            }
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_FieldTransformers_And_FieldGenerators_Construction_Works()
        {
            var fieldTransformers = new Dictionary<string, Func<TableCell, object>>();
            var fieldGenerators = new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>();
            var transformer = new QueryResponseToDataSetTransformer(fieldTransformers, fieldGenerators);
            Assert.IsNotNull(transformer);
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Transform_Is_Called_Without_Any_FiledTransformers_And_FieldGenerators_Data_Is_Copied_As_Is_And_EtlInserTime_Is_Added()
        {
            var schemaAndTable = BuildSchemaAndTable();
            var transformer = new QueryResponseToDataSetTransformer();
            var etlInsertTime = DateTime.Now;
            var result = transformer.Transform(schemaAndTable.schema, schemaAndTable.rows, etlInsertTime);

            var resultTable = result.Tables[0];
            Assert.IsNotNull(resultTable, "Result table is null");
            var schemaMatches = CheckSchemaMatches(schemaAndTable.schema, resultTable);
            Assert.IsTrue(schemaMatches, "Schema does not match");
            var dataMatches = CheckDataMatches(schemaAndTable.rows, resultTable);
            Assert.IsTrue(dataMatches, "Data does not match");
            var etlInsertTimeIsCorrect = CheckEtlInsertTime(resultTable, etlInsertTime);
            Assert.IsTrue(etlInsertTimeIsCorrect, "EtlInsertTime is correct");
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Transform_Is_Called_With_One_FiledTransformers_Data_For_That_Filed_Is_Transformed()
        {
            var schemaAndTable = BuildSchemaAndTable();
            Func<TableCell, object> fieldTransformer = (TableCell c) =>
            {
                var value = c.V as string;
                return value + value;
            };

            var transformer = new QueryResponseToDataSetTransformer(new Dictionary<string, Func<TableCell, object>>
            {
                ["Id"] = fieldTransformer
            });

            var result = transformer.Transform(schemaAndTable.schema, schemaAndTable.rows, DateTime.Now);
            var resultTable = result.Tables[0];

            Assert.IsTrue(CheckSchemaMatches(schemaAndTable.schema, resultTable), "Schema does not match");

            for(var i = 0; i < resultTable.Rows.Count; i++)
            {
                var value = resultTable.Rows[i]["Id"];
                var originalValue = schemaAndTable.rows[i].F[0].V as string;
                Assert.IsTrue(value.Equals(originalValue + originalValue), "Transformed data is invalid");
            }
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Transform_Is_Called_With_One_FiledGenerator_One_New_Field_Is_Derived()
        {
            var schemaAndTable = BuildSchemaAndTable();
            Func<TableCell, object> fieldGenerator= (TableCell c) =>
            {
                var value = c.V as string;
                return value + value;
            };
            var transformer = new QueryResponseToDataSetTransformer(new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>
            {
                [("Id","DoubleId",typeof(string))] = fieldGenerator
            });

            var result = transformer.Transform(schemaAndTable.schema, schemaAndTable.rows, DateTime.Now);
            var resultTable = result.Tables[0];

            Assert.AreEqual(schemaAndTable.schema.Fields.Count + 2, resultTable.Columns.Count);

            for (var i = 0; i < resultTable.Rows.Count; i++)
            {
                var value = resultTable.Rows[i]["DoubleId"];
                var originalValue = schemaAndTable.rows[i].F[0].V as string;
                Assert.IsTrue(value.Equals(originalValue + originalValue), "Transformed data is invalid");
            }
        }

        [TestMethod]
        public void QueryResponseToDataSetTransformerTests_Given_Transform_Is_Called_With_FiledGenerator_On_Non_Existend_Field_Exception_Is_Thrown()
        {
            try
            {
                var schemaAndTable = BuildSchemaAndTable();
                Func<TableCell, object> fieldGenerator = (TableCell c) =>
                {
                    var value = c.V as string;
                    return value + value;
                };
                var transformer = new QueryResponseToDataSetTransformer(new Dictionary<(string source, string destination, Type destinationType), Func<TableCell, object>>
                {
                    [("IDontExist", "DoubleId", typeof(string))] = fieldGenerator
                });

                var result = transformer.Transform(schemaAndTable.schema, schemaAndTable.rows, DateTime.Now);
            }
            catch(Exception ex)
            {
                Assert.IsNotNull(ex, "Expected exception");
            }
        }

        private bool CheckEtlInsertTime(DataTable resultTable, DateTime etlInsertTime)
        {
            for (var i = 0; i < resultTable.Rows.Count; i++)
            {
                var row = resultTable.Rows[i];
                if (((DateTime)row["EtlInsertTime"]) != etlInsertTime) return false;
            }

            return true;
        }

        private bool CheckDataMatches(IList<TableRow> rows, DataTable resultTable)
        {
            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                for (var j = 0; j < row.F.Count; j++)
                {
                    var val = row.F[j].V;
                    var otherVal = resultTable.Rows[i][j];
                    if (!val.Equals(otherVal)) return false;
                }
            }

            return true;
        }

        private bool CheckSchemaMatches(TableSchema schema, DataTable resultTable)
        {
            //+1 for EtlInsertTime
            if (schema.Fields.Count + 1 != resultTable.Columns.Count) return false;
            foreach (var field in schema.Fields) if (!resultTable.Columns.Contains(field.Name)) return false;

            return true;
        }

        private static (TableSchema schema, IList<TableRow> rows) BuildSchemaAndTable()
        {
            var tableSchema = new TableSchema();
            tableSchema.Fields = new List<TableFieldSchema>
            {
                new TableFieldSchema { Name = "Id", Type = "STRING" },
                new TableFieldSchema { Name = "Name", Type = "STRING"},
                new TableFieldSchema { Name = "Timestamp", Type = "INTEGER"}
            };

            var tableRows = new List<TableRow>
            {
                new TableRow
                {
                    F = new List<TableCell> {
                        new TableCell { V = "1" },
                        new TableCell { V = "FirstName" },
                        new TableCell { V = (long)123345678 }
                    }
                },
                new TableRow
                {
                    F = new List<TableCell> {
                        new TableCell { V = "2" },
                        new TableCell { V = "SecondName" },
                        new TableCell { V = (long)123345679 }
                    }
                },
                new TableRow
                {
                    F = new List<TableCell> {
                    new TableCell { V = "3" },
                    new TableCell { V = "ThirdName" },
                    new TableCell { V = (long)123345680 }
                    }
                }
            };

            return (tableSchema, tableRows);
        }
    }
}
