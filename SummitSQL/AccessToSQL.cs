using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        /// <summary>
        /// Retrieves a list of all tables within the Access database.
        /// </summary>
        /// <returns>A DataTable containing information on all tables.</returns>
        public DataTable GetAccessTables()
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                connection.Open();
                return connection.GetSchema("Tables");
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
    /// Manages database schema operations for SQL Server.
    /// </summary>
    public class SqlServerSchemaManager
    {
        private readonly string _sqlConnectionString;

        public SqlServerSchemaManager(string sqlConnectionString)
        {
            _sqlConnectionString = sqlConnectionString;
        }

        /// <summary>
        /// Ensures that a table exists in the SQL Server database. It sanitizes the table name
        /// by replacing spaces with hyphens and handles SQL execution errors.
        /// </summary>
        /// <param name="columns">DataTable containing the schema for the table columns.</param>
        /// <param name="tableName">The original name of the table as retrieved from Access.</param>
        public void CreateTable(DataTable columns, string tableName)
        {
            string sanitizedTableName = SanitizeTableName(tableName);

            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                string createTableQuery = BuildCreateTableQuery(columns, sanitizedTableName);

                try
                {
                    using (var command = new SqlCommand(createTableQuery, connection))
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine($"Table '{sanitizedTableName}' created in SQL Server.");
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Failed to create table '{sanitizedTableName}': {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Sanitizes the table name to be compliant with SQL Server naming conventions.
        /// This method replaces spaces with hyphens to ensure SQL Server can process the names without errors.
        /// </summary>
        /// <param name="tableName">The original table name.</param>
        /// <returns>A sanitized table name suitable for SQL Server.</returns>
        private string SanitizeTableName(string tableName)
        {
            return tableName.Replace(" ", "-");
        }

        /// <summary>
        /// Builds the SQL command for creating a table based on the given columns and the sanitized table name.
        /// </summary>
        /// <param name="columns">DataTable with column definitions.</param>
        /// <param name="sanitizedTableName">Sanitized table name.</param>
        /// <returns>SQL command string for creating the table.</returns>
        private string BuildCreateTableQuery(DataTable columns, string sanitizedTableName)
        {
            StringBuilder createTableQuery = new StringBuilder($"CREATE TABLE [{sanitizedTableName}] (");
            foreach (DataRow column in columns.Rows)
            {
                string columnName = column["COLUMN_NAME"].ToString();
                string dataType = column["DATA_TYPE"].ToString();
                string sqlDataType = ConvertToSqlDataType(dataType);
                createTableQuery.Append($"[{columnName}] {sqlDataType}, ");
            }
            createTableQuery.Length -= 2; // Remove the trailing comma and space
            createTableQuery.Append(")");

            return createTableQuery.ToString();
        }

        /// <summary>
        /// Converts Access data types to SQL Server data types.
        /// </summary>
        /// <param name="accessDataType">Access data type as a string.</param>
        /// <returns>SQL Server data type as a string.</returns>
        private string ConvertToSqlDataType(string accessDataType)
        {
            switch (accessDataType)
            {
                case "DBTYPE_I4":
                    return "INT";
                case "DBTYPE_DBDATE":
                    return "DATE";
                default:
                    return "NVARCHAR(MAX)";
            }
        }
    }
}
