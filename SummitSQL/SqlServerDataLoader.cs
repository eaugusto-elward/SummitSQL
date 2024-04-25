using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;

/// <summary>
/// Responsible for transferring data from memory into a SQL Server database.
/// </summary>
public class SqlServerDataLoader
{
    private readonly string _sqlConnectionString;
    private readonly IMemoryCache _cache;
    private readonly List<string> _tableNames;

    /// <summary>
    /// Constructor for initializing the SqlServerDataLoader with necessary dependencies.
    /// </summary>
    /// <param name="sqlConnectionString">Connection string for the SQL Server database.</param>
    /// <param name="cache">Cache service to retrieve stored data.</param>
    /// <param name="tableNames">List of table names that have been loaded into memory.</param>
    public SqlServerDataLoader(string sqlConnectionString, IMemoryCache cache, List<string> tableNames)
    {
        _sqlConnectionString = sqlConnectionString;
        _cache = cache;
        _tableNames = tableNames;
    }

    /// <summary>
    /// Transfers all cached data to the SQL Server database using bulk insert operations.
    /// </summary>
    public void TransferDataToSqlServer()
    {
        foreach (var tableName in _tableNames)
        {
            if (_cache.TryGetValue(tableName, out DataTable dataTable))
            {
                BulkInsert(dataTable, tableName);
            }
        }
    }

    /// <summary>
    /// Performs a bulk insert of data into a specific table in SQL Server.
    /// </summary>
    /// <param name="dataTable">Data table containing the data to insert.</param>
    /// <param name="tableName">The name of the destination table in SQL Server.</param>
    private void BulkInsert(DataTable dataTable, string tableName)
    {
        using (var connection = new SqlConnection(_sqlConnectionString))
        {
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = $"[{tableName}]"; // Ensure tableName is already sanitized
                        bulkCopy.BatchSize = 1000;
                        bulkCopy.WriteToServer(dataTable);
                        transaction.Commit();
                        Console.WriteLine($"Successfully transferred data to {tableName}");
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine($"Error transferring data to {tableName}: {ex.Message}");
                }
            }
        }
    }

}
