using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;
using System.Text;

/// <summary>
/// Responsible for loading data from an Access database into memory.
/// </summary>
public class AccessDataLoader
{
    private readonly string _connectionString;
    private readonly IMemoryCache _cache;
    private readonly List<string> _tableNames;

    /// <summary>
    /// Constructor for initializing the AccessDataLoader with necessary dependencies.
    /// </summary>
    /// <param name="connectionString">Connection string for the Access database.</param>
    /// <param name="cache">Cache service for storing data.</param>
    /// <param name="tableNames">List to keep track of loaded table names.</param>
    public AccessDataLoader(string connectionString, IMemoryCache cache, List<string> tableNames)
    {
        _connectionString = connectionString;
        _cache = cache;
        _tableNames = tableNames;
    }

    /// <summary>
    /// Loads all tables from the Access database into memory.
    /// </summary>
    public void LoadAllTablesIntoMemory()
    {
        Log.Information("Loading Tables Into Memory");
        using (var connection = new OleDbConnection(_connectionString))
        {
            connection.Open();
            var schemaTable = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
            foreach (DataRow row in schemaTable.Rows)
            {
                var tableName = row["TABLE_NAME"].ToString();
                LoadTableData(connection, tableName);
            }
        }
    }

    /// <summary>
    /// Loads data from a specific table into the cache.
    /// </summary>
    /// <param name="connection">Active OleDbConnection.</param>
    /// <param name="tableName">Name of the table to load data from.</param>
    private void LoadTableData(OleDbConnection connection, string tableName)
    {
        using (var command = new OleDbCommand($"SELECT * FROM [{tableName}]", connection))
        using (var adapter = new OleDbDataAdapter(command))
        {
            var table = new DataTable();
            adapter.Fill(table);
            _cache.Set(SanitizeTableName(tableName), table, new MemoryCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromHours(1)
            });
            Console.WriteLine($"Loaded and cached {table.Rows.Count} rows from {SanitizeTableName(tableName)}.");
            _tableNames.Add(SanitizeTableName(tableName));
        }
    }

    /// <summary>
    /// Sanitizes the table name to replace spaces with hyphens.
    /// </summary>
    /// <param name="tableName">The original table name.</param>
    /// <returns>A sanitized table name suitable for use in SQL queries.</returns>
    private string SanitizeTableName(string tableName)
    {
        return tableName.Replace(" ", "-");
    }

    public bool CheckAndUpdateTable(string tableName)
    {
        var newData = LoadTableDataDirectly(tableName);
        if (!_cache.TryGetValue(tableName, out DataTable currentData) || !TablesMatch(currentData, newData, tableName))
        {
            _cache.Set(tableName, newData);
            return true;
        }
        return false;
    }

    private DataTable LoadTableDataDirectly(string tableName)
    {
        using (var connection = new OleDbConnection(_connectionString))
        {
            connection.Open();
            var cmd = new OleDbCommand($"SELECT * FROM [{tableName}]", connection);
            var adapter = new OleDbDataAdapter(cmd);
            var dt = new DataTable();
            adapter.Fill(dt);
            return dt;
        }
    }

    /// <summary>
    /// Compares two DataTables to determine if they are identical.
    /// </summary>
    /// <param name="dt1">First DataTable to compare.</param>
    /// <param name="dt2">Second DataTable to compare.</param>
    /// <param name="tableName">The name of the table being compared.</param>
    /// <returns>True if the tables match; otherwise, false.</returns>
    private bool TablesMatch(DataTable sqlData, DataTable cachedData, string tableName)
    {
        Log.Information("Begin Table Matching");

        if (sqlData.Rows.Count != cachedData.Rows.Count)
        {
            Log.Information("Row count mismatch in {TableName}: SQL Server has {SqlRowCount} rows, Cache has {CacheRowCount} rows",
                            tableName, sqlData.Rows.Count, cachedData.Rows.Count);
            return false;
        }

        for (int i = 0; i < sqlData.Rows.Count; i++)
        {
            for (int j = 0; j < sqlData.Columns.Count; j++)
            {
                if (!Equals(sqlData.Rows[i][j], cachedData.Rows[i][j]))
                {
                    Log.Information("Data mismatch in {TableName} at row {Row}, column {Column}: SQL Server='{SqlValue}', Cache='{CacheValue}'",
                                    tableName, i + 1, sqlData.Columns[j].ColumnName, sqlData.Rows[i][j], cachedData.Rows[i][j]);
                    return false;
                }
            }
        }
        return true; // If all rows and columns match, return true
    }

    /// <summary>
    /// Logs a change detected in a table column.
    /// </summary>
    /// <param name="tableName">Table where the change occurred.</param>
    /// <param name="timestamp">Timestamp of the change.</param>
    /// <param name="columnName">Column that changed.</param>
    /// <param name="oldValue">Old value of the column.</param>
    /// <param name="newValue">New value of the column.</param>
    private void LogChange(string tableName, DateTime timestamp, string columnName, object oldValue, object newValue)
    {
        Log.Information("Change detected in table {TableName} at {Timestamp}: Column {ColumnName} changed from {OldValue} to {NewValue}",
                        tableName, timestamp, columnName, oldValue, newValue);
    }

    /// <summary>
    /// Prints the cached data for all loaded tables.
    /// </summary>
    public void PrintCachedData()
    {
        // Commented out varaible below is used for specific debugging purposes
        // Comment out the foreach and use the variable to print a specific table from cache
        foreach (var tableName in _tableNames)
        {
        // var tableName = "tblPrinters";
        if (_cache.TryGetValue(tableName, out DataTable table))
            {
                Console.WriteLine($"Data for table {tableName}:");
                Log.Information($"Data for table {tableName}:");
                PrintDataTable(table);
            }
            else
            {
                Console.WriteLine($"No data found in cache for table {tableName}.");
                Log.Information($"No data found in cache for table {tableName}.");
            }
        }
    }

    /// <summary>
    /// Prints the data of a DataTable to the console and logs it.
    /// </summary>
    /// <param name="table">The DataTable to print.</param>
    private void PrintDataTable(DataTable table)
    {
        // Create a string to accumulate the data for logging and console output
        StringBuilder dataBuilder = new StringBuilder();
        dataBuilder.AppendLine(String.Join("\t", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

        foreach (DataRow row in table.Rows)
        {
            dataBuilder.AppendLine(String.Join("\t", row.ItemArray));
        }

        // Output to console and log file
        Console.WriteLine(dataBuilder.ToString());
        Log.Information(dataBuilder.ToString());
    }

}


