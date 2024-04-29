using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;

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
                        bulkCopy.DestinationTableName = $"[{tableName}]";
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

    /// <summary>
    /// Updates SQL Server with the latest data for a specific table.
    /// </summary>
    /// <param name="tableName">Name of the table in SQL Server.</param>
    /// <param name="dataTable">Data table containing the updated data.</param>
    public void UpdateSqlServer(string tableName, DataTable dataTable)
    {
        try
        {
            // Clear existing data
            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                connection.Open();
                var command = new SqlCommand($"DELETE FROM [{tableName}]", connection);
                command.ExecuteNonQuery();
            }

            // Insert updated data
            BulkInsert(dataTable, tableName);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error updating data in SQL Server for table {tableName}: {ex.Message}");
        }
    }

    public void VerifyDataConsistency(string tableName)
    {
        Log.Information("Start Verify");

        DataTable cachedData;
        if (_cache.TryGetValue(tableName, out cachedData))
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
                    Log.Information($"Data mismatch for {tableName}: SQL Server rows {sqlData.Rows.Count}, Cached rows {cachedData.Rows.Count}.");
                    return;
                }

                for (int i = 0; i < sqlData.Rows.Count; i++)
                {
                    for (int j = 0; j < sqlData.Columns.Count; j++)
                    {
                        var sqlValue = Convert.ToString(sqlData.Rows[i][j]).Trim();
                        var cachedValue = Convert.ToString(cachedData.Rows[i][j]).Trim();
                        if (!string.Equals(sqlValue, cachedValue, StringComparison.Ordinal))
                        {
                            Console.WriteLine($"Data mismatch in {tableName} at row {i + 1} column {sqlData.Columns[j].ColumnName}, SQL Server value: '{sqlValue}', Cached value: '{cachedValue}'.");
                            Log.Information($"Data mismatch in {tableName} at row {i + 1}, column {sqlData.Columns[j].ColumnName}, SQL Server value: '{sqlValue}', Cached value: '{cachedValue}'.");
                            return;
                        }
                    }
                }

                Console.WriteLine($"Data for {tableName} is consistent between SQL Server and cache.");
                Log.Information($"Data for {tableName} is consistent between SQL Server and cache.");
            }
        }
        else
        {
            Console.WriteLine($"No cached data found for {tableName}.");
            Log.Information($"No cached data found for {tableName}.");
        }
    }

}
