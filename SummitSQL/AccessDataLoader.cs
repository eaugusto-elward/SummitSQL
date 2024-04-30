using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;
using System.Text;

/// <summary>
/// Responsible for loading data from an Access database into memory.
/// This class handles connections to the Access database, reads data from tables, and caches the data for further processing.
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
    /// This method retrieves the schema of the database, iterates over the tables, and loads each one into the cache.
    /// </summary>
    public void LoadAllTablesIntoMemory()
    {
        Log.Information("Loading all tables from Access database into memory.");
        using (var connection = new OleDbConnection(_connectionString))
        {
            connection.Open();
            var schemaTable = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });

            foreach (DataRow row in schemaTable.Rows)
            {
                var tableName = row["TABLE_NAME"].ToString();
                if (!tableName.StartsWith("MSys"))  // Excludes system tables which start with 'MSys'
                {
                    LoadTableData(connection, tableName);
                }
            }
        }
    }

    /// <summary>
    /// Loads data from a specified table into the cache.
    /// </summary>
    /// <param name="connection">Active OleDbConnection to use for querying.</param>
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
                SlidingExpiration = TimeSpan.FromHours(1)  // Sets cache expiration to 1 hour
            });
            _tableNames.Add(SanitizeTableName(tableName));
            Log.Information($"Loaded {table.Rows.Count} rows from {tableName} into cache.");
        }
    }

    /// <summary>
    /// Sanitizes the table name to ensure it can be safely used in SQL queries and as cache keys.
    /// </summary>
    /// <param name="tableName">The original table name.</param>
    /// <returns>A sanitized table name suitable for use in SQL queries and caching.</returns>
    private string SanitizeTableName(string tableName)
    {
        var sanitized = tableName.Replace(" ", "-");
        Log.Information($"Sanitized table name from '{tableName}' to '{sanitized}'.");
        return sanitized;
    }

    /// <summary>
    /// Checks and updates a specific table in memory if the data has changed.
    /// </summary>
    /// <param name="tableName">Name of the table to check and update.</param>
    /// <returns>True if the table was updated, false otherwise.</returns>
    public bool CheckAndUpdateTable(string tableName)
    {
        var newData = LoadTableDataDirectly(tableName);
        if (!_cache.TryGetValue(tableName, out DataTable currentData) || !TablesMatch(currentData, newData, tableName))
        {
            _cache.Set(tableName, newData);
            Log.Information($"Table {tableName} updated in cache.");
            return true;
        }
        return false;
    }

    /// <summary>
    /// Loads data directly from the Access database for a specific table.
    /// </summary>
    /// <param name="tableName">Name of the table to load data from.</param>
    /// <returns>A DataTable containing the data from the specified table.</returns>
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

    /// <summary>
    /// Compares two DataTables to determine if they match exactly, row by row and column by column.
    /// </summary>
    /// <param name="dt1">The first DataTable.</param>
    /// <param name="dt2">The second DataTable.</param>
    /// <param name="tableName">Name of the table being compared for logging purposes.</param>
    /// <returns>True if the tables match; otherwise, false.</returns>
    private bool TablesMatch(DataTable dt1, DataTable dt2, string tableName)
    {
        if (dt1.Rows.Count != dt2.Rows.Count)
        {
            Log.Information($"Mismatch in row count for {tableName}: {dt1.Rows.Count} vs {dt2.Rows.Count}");
            return false;
        }

        for (int i = 0; i < dt1.Rows.Count; i++)
        {
            for (int j = 0; j < dt1.Columns.Count; j++)
            {
                if (!Equals(dt1.Rows[i][j], dt2.Rows[i][j]))
                {
                    Log.Information($"Data mismatch in {tableName} at row {i + 1}, column {dt1.Columns[j].ColumnName}: '{dt1.Rows[i][j]}' vs '{dt2.Rows[i][j]}'");
                    return false;
                }
            }
        }
        return true;
    }
}
