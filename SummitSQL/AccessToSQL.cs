using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Text;

namespace SummitSQL
{
    /// <summary>
    /// Handles schema operations for an Access database.
    /// </summary>
    public class AccessDatabaseSchemaManager
    {
        private readonly string _connectionString;

        public AccessDatabaseSchemaManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DataTable GetAccessTables()
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                connection.Open();
                return connection.GetSchema("Tables");
            }
        }

        public DataTable GetTableColumns(string tableName)
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                connection.Open();
                return connection.GetSchema("Columns", new[] { null, null, tableName });
            }
        }
    }

    /// <summary>
    /// Manages database schema operations for SQL Server.
    /// </summary>
    public class SqlServerSchemaManager
    {
        private readonly string _sqlConnectionString;

        public SqlServerSchemaManager(string sqlConnectionString)
        {
            _sqlConnectionString = sqlConnectionString;
        }

        public void CreateTable(DataTable columns, string tableName)
        {
            string sanitizedTableName = SanitizeTableName(tableName);
            string createTableQuery = BuildCreateTableQuery(columns, sanitizedTableName);

            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(createTableQuery, connection))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine($"Table '{sanitizedTableName}' created in SQL Server.");
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine($"Failed to create table '{sanitizedTableName}': {ex.Message}");
                    }
                }
            }
        }

        private string SanitizeTableName(string tableName)
        {
            return tableName.Replace(" ", "-");
        }

        private string BuildCreateTableQuery(DataTable columns, string sanitizedTableName)
        {
            StringBuilder createTableQuery = new StringBuilder($"CREATE TABLE [{sanitizedTableName}] (");

            foreach (DataRow column in columns.Rows)
            {
                string columnName = column["COLUMN_NAME"].ToString();
                string sqlDataType = ConvertToSqlDataType(column["DATA_TYPE"].ToString(), column["CHARACTER_MAXIMUM_LENGTH"]);
                createTableQuery.Append($"[{columnName}] {sqlDataType}, ");
            }

            createTableQuery.Length -= 2;  // Remove the trailing comma and space
            createTableQuery.Append(")");
            return createTableQuery.ToString();
        }

        private string ConvertToSqlDataType(string accessDataType, object maxLength)
        {
            int length = maxLength != DBNull.Value ? Convert.ToInt32(maxLength) : -1;

            switch (accessDataType)
            {
                case "DBTYPE_I4":
                case "DBTYPE_I2":
                    return "INT";
                case "DBTYPE_DBDATE":
                case "DBTYPE_DBTIMESTAMP":
                    return "DATETIME";
                case "DBTYPE_R8":
                    return "FLOAT";
                case "DBTYPE_BOOL":
                    return "BIT";
                case "DBTYPE_GUID":
                    return "UNIQUEIDENTIFIER";
                case "DBTYPE_BYTES":
                    return "VARBINARY(MAX)";
                default:
                    // Ensure that length is not zero or negative, which is invalid in SQL Server for types that require length specifications.
                    if (length <= 0)
                    {
                        // Defaulting to a sensible length or using MAX for unspecified or invalid lengths
                        return "NVARCHAR(MAX)";
                    }
                    else
                    {
                        return $"NVARCHAR({length})";
                    }
            }
        }

    }
}
