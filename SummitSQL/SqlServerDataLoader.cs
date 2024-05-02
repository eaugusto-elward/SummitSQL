using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using System.Linq;
using Serilog;

/// <summary>
/// Responsible for transferring data from an in-memory cache to a SQL Server database.
/// Manages the SQL Server connection and facilitates the bulk insertion of data.
/// </summary>
public class SqlServerDataLoader
{
    private readonly string _sqlConnectionString;
    private readonly IMemoryCache _cache;
    private readonly List<string> _tableNames;

    /// <summary>
    /// Initializes a new instance of the SqlServerDataLoader with necessary dependencies.
    /// </summary>
    /// <param name="sqlConnectionString">Connection string for the SQL Server database.</param>
    /// <param name="cache">Cache service to retrieve stored data.</param>
    /// <param name="tableNames">List of table names that have been loaded into memory and need to be synchronized.</param>
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
        Log.Information("Starting data transfer to SQL Server.");
        foreach (var tableName in _tableNames)
        {
            if (_cache.TryGetValue(tableName, out DataTable dataTable))
            {
                if (dataTable == null || dataTable.Rows.Count == 0)
                {
                    Log.Warning($"No rows found in dataTable for {tableName}. Skipping bulk insert.");
                    continue;
                }

                Log.Information($"Proceeding with bulk insert for {tableName} with {dataTable.Rows.Count} rows.");
                BulkInsert(dataTable, tableName);
            }
            else
            {
                Log.Warning($"No dataTable found in cache for {tableName}. Skipping this table.");
            }
        }
    }

    /// <summary>
    /// Performs a bulk insert of data into a specific table in SQL Server.
    /// Efficiently transfers large volumes of data and ensures transactional integrity.
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
                        bulkCopy.DestinationTableName = $"[{tableName}]";
                        bulkCopy.BatchSize = 1000;
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                            Log.Information($"Mapping column {column.ColumnName} to {column.ColumnName} in {tableName}.");
                        }

                        bulkCopy.WriteToServer(dataTable);
                        transaction.Commit();
                        Log.Information($"Successfully transferred data to {tableName} with {dataTable.Rows.Count} rows.");
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Log.Error($"Data transfer to {tableName} failed. Error: {ex.Message}");
                    LogDetailedError(dataTable, ex);
                    throw; // Rethrow the exception to handle it further up if necessary
                }
            }
        }
    }

    /// <summary>
    /// Logs detailed information about an error during data transfer to assist with troubleshooting.
    /// </summary>
    /// <param name="dataTable">The data table that was being processed when the error occurred.</param>
    /// <param name="ex">The exception that was thrown.</param>
    private void LogDetailedError(DataTable dataTable, Exception ex)
    {
        foreach (DataRow row in dataTable.Rows)
        {
            var rowValues = row.ItemArray.Select(r => r.ToString()).ToArray();
            Log.Information($"Problematic row data: {string.Join(", ", rowValues)}");
        }
    }
}
