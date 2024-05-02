using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Text;
using Serilog;

namespace SummitSQL
{
    /// <summary>
    /// Handles schema operations for an Access database, retrieving information about tables and their columns.
    /// </summary>
    public class AccessDatabaseSchemaManager
    {
        private readonly string _connectionString;

        /// <summary>
        /// Initializes a new instance of the AccessDatabaseSchemaManager with a specific connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to the Access database.</param>
        public AccessDatabaseSchemaManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Retrieves a list of all tables within the Access database.
        /// </summary>
        /// <returns>A DataTable containing information on all tables.</returns>
        public DataTable GetAccessTables()
        {
            if (OperatingSystem.IsWindows())
            {
                using (var connection = new OleDbConnection(_connectionString))
                {
                    connection.Open();
                    return connection.GetSchema("Tables");
                }
            }
            else
            {
                Log.Error("Access database schema operations are only supported on Windows.");
                return null;
            }
        }

        /// <summary>
        /// Retrieves column information for a specific table in the Access database.
        /// </summary>
        /// <param name="tableName">The name of the table to retrieve columns for.</param>
        /// <returns>A DataTable containing column information.</returns>
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
    /// Manages database schema operations for SQL Server, ensuring that tables are created and updated based on Access database schemas.
    /// </summary>
    public class SqlServerSchemaManager
    {
        private readonly string _sqlConnectionString;

        /// <summary>
        /// Initializes a new instance of the SqlServerSchemaManager with a specific SQL Server connection string.
        /// </summary>
        /// <param name="sqlConnectionString">The connection string to the SQL Server database.</param>
        public SqlServerSchemaManager(string sqlConnectionString)
        {
            _sqlConnectionString = sqlConnectionString;
        }

        /// <summary>
        /// Creates a table in SQL Server based on the schema defined in an Access database table.
        /// </summary>
        /// <param name="columns">DataTable containing the schema for the table columns.</param>
        /// <param name="tableName">The original name of the table as retrieved from Access.</param>
        public void CreateTable(DataTable columns, string tableName)
        {
            string sanitizedTableName = SanitizeTableName(tableName);
            StringBuilder createTableQuery = new StringBuilder($"CREATE TABLE [{sanitizedTableName}] (");

            foreach (DataRow column in columns.Rows)
            {
                string columnName = column["COLUMN_NAME"].ToString();
                string accessDataType = column["DATA_TYPE"].ToString();
                int? characterMaxLength = column.Table.Columns.Contains("CHARACTER_MAXIMUM_LENGTH") && column["CHARACTER_MAXIMUM_LENGTH"] != DBNull.Value
                                            ? Convert.ToInt32(column["CHARACTER_MAXIMUM_LENGTH"])
                                            : null;

                string sqlDataType = ConvertToSqlDataType(accessDataType, characterMaxLength);
                createTableQuery.Append($"[{columnName}] {sqlDataType}, ");
            }

            createTableQuery.Length -= 2; // Remove the trailing comma and space
            createTableQuery.Append(")");

            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(createTableQuery.ToString(), connection))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                        Log.Information($"Table '{sanitizedTableName}' created in SQL Server.");
                    }
                    catch (SqlException ex)
                    {
                        Log.Error($"Failed to create table '{sanitizedTableName}': {ex.Message}");
                        Console.WriteLine($"Failed to create table '{sanitizedTableName}': {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Sanitizes the table name to be compliant with SQL Server naming conventions.
        /// </summary>
        /// <param name="tableName">The original table name.</param>
        /// <returns>A sanitized table name suitable for SQL Server.</returns>
        private string SanitizeTableName(string tableName)
        {
            return tableName.Replace(" ", "-");
        }

        /// <summary>
        /// Converts Access data types to SQL Server data types to ensure compatibility.
        /// </summary>
        /// <param name="accessDataType">Access data type as a string.</param>
        /// <param name="characterMaxLength">Maximum length of characters for string data types, if applicable.</param>
        /// <returns>SQL Server data type as a string.</returns>
        private string ConvertToSqlDataType(string accessDataType, int? characterMaxLength)
        {
            switch (accessDataType)
            {
                case "DBTYPE_I4":
                case "Long Integer":
                    return "INT";
                case "DBTYPE_R8": // Double in Access
                    return "FLOAT";
                case "Text":
                    return characterMaxLength.HasValue && characterMaxLength > 0 ? $"NVARCHAR({characterMaxLength.Value})" : "NVARCHAR(255)";
                case "Memo":
                    return "NVARCHAR(MAX)";
                default:
                    return "NVARCHAR(MAX)"; // Default fallback for all other types
            }
        }
    }
}
