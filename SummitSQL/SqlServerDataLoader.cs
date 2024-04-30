using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;

/// <summary>
/// Responsible for transferring data from memory into a SQL Server database.
/// This class manages the connection to SQL Server and facilitates the bulk insertion of data from the in-memory cache.
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
        Log.Information("Starting data transfer to SQL Server.");
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
    /// This method is designed to efficiently transfer large volumes of data.
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
                        }

                        bulkCopy.WriteToServer(dataTable);
                        transaction.Commit();
                        Log.Information($"Successfully transferred data to {tableName}.");
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine($"Error transferring data to {tableName}: {ex.Message}");
                    Log.Error($"Data transfer to {tableName} failed. Error: {ex.Message}");
                    LogDetailedError(dataTable, ex);
                }
            }
        }
    }

    /// <summary>
    /// Logs detailed information about an error during data transfer.
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

    /// <summary>
    /// Updates SQL Server with the latest data for a specific table.
    /// This method clears existing data and inserts updated data to maintain data integrity.
    /// </summary>
    /// <param name="tableName">Name of the table in SQL Server.</param>
    /// <param name="dataTable">Data table containing the updated data.</param>
    public void UpdateSqlServer(string tableName, DataTable dataTable)
    {
        Log.Information($"Updating data for {tableName} in SQL Server.");
        try
        {
            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand($"DELETE FROM [{tableName}]", connection))
                {
                    command.ExecuteNonQuery();
                }

                BulkInsert(dataTable, tableName);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error updating data in SQL Server for table {tableName}: {ex.Message}");
            Log.Error($"Error updating data in SQL Server for table {tableName}: {ex.Message}");
        }
    }

    /// <summary>
    /// Verifies the data consistency between the SQL Server database and the cached data.
    /// </summary>
    /// <param name="tableName">Name of the table to verify.</param>
    public void VerifyDataConsistency(string tableName)
    {
        Log.Information($"Starting data consistency verification for {tableName}.");
        if (_cache.TryGetValue(tableName, out DataTable cachedData))
        {
            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                var command = new SqlCommand($"SELECT * FROM [{tableName}]", connection);
                var adapter = new SqlDataAdapter(command);
                var sqlData = new DataTable();
                adapter.Fill(sqlData);

                if (sqlData.Rows.Count != cachedData.Rows.Count)
                {
                    Console.WriteLine($"Data mismatch for {tableName}: Row counts differ.");
                    return;
                }

                for (int i = 0; i < sqlData.Rows.Count; i++)
                {
                    foreach (DataColumn column in sqlData.Columns)
                    {
                        object sqlValue = sqlData.Rows[i][column.ColumnName];
                        object cachedValue = cachedData.Rows[i][column.ColumnName];

                        if (!Convert.ToString(sqlValue).Equals(Convert.ToString(cachedValue)))
                        {
                            Console.WriteLine($"Data mismatch in {tableName} at row {i + 1}, column {column.ColumnName}, SQL Server value: '{sqlValue}', Cached value: '{cachedValue}'.");
                            return;
                        }
                    }
                }

                Console.WriteLine($"Data for {tableName} is consistent between SQL Server and cache.");
            }
        }
        else
        {
            Console.WriteLine($"No cached data found for {tableName}.");
        }
    }
}
