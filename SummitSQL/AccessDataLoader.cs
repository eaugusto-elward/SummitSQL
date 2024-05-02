using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;
using System.Text;

/// <summary>
/// Manages data loading from an Access database and stores it into a memory cache.
/// This class is responsible for connecting to the database, reading data from tables, and caching the data for further processing.
/// </summary>
public class AccessDataLoader
{
    private string _connectionString;
    private readonly IMemoryCache _cache;
    //private readonly List<string> _tableNames;
    public Dictionary<string, string> TableNames { get; private set; } // Expose table names

    /// <summary>
    /// Initializes a new instance of the AccessDataLoader class.
    /// </summary>
    /// <param name="connectionString">The connection string to the Access database.</param>
    /// <param name="cache">The memory cache to store table data.</param>
    public AccessDataLoader(string connectionString, IMemoryCache cache)
    {
        _connectionString = connectionString;
        _cache = cache;
        TableNames = TableNames ?? new Dictionary<string, string>();
    }

    /// <summary>
    /// Loads all user tables from the Access database into the memory cache.
    /// </summary>
    public void LoadAllTablesIntoMemory()
    {
        Log.Information("Loading all tables from Access database into memory.");

        if (OperatingSystem.IsWindows())
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                connection.Open();
                var schemaTable = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });

                foreach (DataRow row in schemaTable.Rows)
                {
                    var originalName = row["TABLE_NAME"].ToString();
                    if (!originalName.StartsWith("MSys"))
                    {
                        var sanitized = SanitizeTableName(originalName);
                        LoadTableData(connection, originalName, sanitized);
                        if (!TableNames.ContainsKey(originalName))
                            TableNames.Add(originalName, sanitized);

                    }
                }
            }
        }
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("Access Database loaded into memory successfully.");
        Console.ResetColor();
        Log.Information("Access Database loaded into memory successfully.");
    }

    /// <summary>
    /// Loads data from a specified table into the cache.
    /// </summary>
    /// <param name="connection">An active OleDbConnection to use for querying the database.</param>
    /// <param name="originalName">The original name of the table as retrieved from the database.</param>
    /// <param name="sanitized">The sanitized name of the table for use in SQL queries and caching.</param>
    /// </summary>
    private void LoadTableData(OleDbConnection connection, string originalName, string sanitized)
    {
        bool success = false;
        int retryCount = 0;
        while (!success && retryCount < 5) // Retry up to 5 times
        {
            try
            {
                using (var command = new OleDbCommand($"SELECT * FROM [{originalName}]", connection))
                using (var adapter = new OleDbDataAdapter(command))
                {
                    var table = new DataTable();
                    adapter.Fill(table);
                    success = true;
                    _cache.Set(sanitized, table, new MemoryCacheEntryOptions
                    {
                        Priority = CacheItemPriority.High,
                        SlidingExpiration = TimeSpan.FromHours(17)
                    });
                    Log.Information($"Loaded {table.Rows.Count} rows from {originalName} into cache.");
                }
            }
            catch (OleDbException ex) when (ex.Message.Contains("already opened exclusively"))
            {
                retryCount++;
                Thread.Sleep(1000); // Wait for 1 second before retrying
                Log.Information($"Retrying to load table {originalName}. Attempt {retryCount}");
                if (retryCount >= 5)
                {
                    Log.Error($"Failed to load table {originalName} after 5 attempts.");
                    Log.Error(ex.Message);
                }
            }
            catch (OleDbException ex)
            {
                Log.Error($"Failed to load table {originalName}: {ex.Message}");
                Log.Error(ex.Message);
                retryCount = 5; // Exit loop after first failure
            }
            catch (Exception ex)
            {
                Log.Error($"Failed to load table {originalName}: {ex.Message}");
                Log.Error(ex.Message);
                retryCount = 5; // Exit loop after first failure
            }
        }
    }


    /// <summary>
    /// Sanitizes the table name for use in SQL queries and as a cache key.
    /// </summary>
    /// <param name="tableName">The original table name.</param>
    /// <returns>A sanitized version of the table name suitable for SQL queries and caching.</returns>
    /// </summary>
    public string SanitizeTableName(string tableName)
    {
        var sanitized = tableName.Replace(" ", "-").Replace("'", ""); // Replaces spaces and apostrophes
        Log.Information($"Sanitized table name from '{tableName}' to '{sanitized}'.");
        return sanitized;
    }

    /// <summary>
    /// Checks if the data in the cache for a specified table needs to be updated, and updates it if necessary.
    /// </summary>
    /// <param name="tableName">The name of the table to check and update.</param>
    /// <returns>True if the table was updated, false otherwise.</returns>
    /// </summary>
    public bool CheckAndUpdateTable(string tableName)
    {
        var newData = LoadTableDataDirectly(tableName);
        if (!_cache.TryGetValue(tableName, out DataTable currentData) || !TablesMatch(currentData, newData))
        {
            _cache.Set(tableName, newData);
            Log.Information($"Table {tableName} updated in cache.");
            return true;
        }
        return false;
    }

    /// <summary>
    /// Directly loads data from the Access database for a specified table.
    /// </summary>
    /// <param name="tableName">The name of the table to load data from.</param>
    /// <returns>A DataTable containing the data from the specified table.</returns>
    /// </summary>
    public DataTable LoadTableDataDirectly(string tableName)
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
    /// Prints cached data for all loaded tables to the console.
    /// </summary>
    public void PrintCachedData()
    {
        foreach (var tableName in TableNames)
        {
            if (_cache.TryGetValue(tableName, out DataTable table))
            {
                Console.WriteLine($"Data for table {tableName}:");
                PrintDataTable(table);
            }
            else
            {
                Console.WriteLine($"No data found in cache for table {tableName}.");
            }
        }
    }

    /// <summary>
    /// Prints the contents of a DataTable to the console.
    /// </summary>
    /// <param name="table">The DataTable to print.</param>
    private void PrintDataTable(DataTable table)
    {
        StringBuilder dataBuilder = new StringBuilder();
        dataBuilder.AppendLine(String.Join("\t", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));
        foreach (DataRow row in table.Rows)
        {
            dataBuilder.AppendLine(String.Join("\t", row.ItemArray));
        }
        Console.WriteLine(dataBuilder.ToString());
    }

    /// <summary>
    /// Compares two DataTables to determine if they are exactly the same.
    /// </summary>
    /// <param name="dt1">The first DataTable to compare.</param>
    /// <param name="dt2">The second DataTable to compare.</param>
    /// <returns>True if the tables match; otherwise, false.</returns>
    public bool TablesMatch(DataTable dt1, DataTable dt2)
    {
        if (dt1.Rows.Count != dt2.Rows.Count)
        {
            Log.Information($"Mismatch in row count: {dt1.Rows.Count} vs {dt2.Rows.Count}");

            return false;
        }
        for (int i = 0; i < dt1.Rows.Count; i++)
        {
            for (int j = 0; j < dt1.Columns.Count; j++)
            {
                if (!Equals(dt1.Rows[i][j], dt2.Rows[i][j]))
                {
                    Log.Information($"Data mismatch at row {i + 1}, column {dt1.Columns[j].ColumnName}: '{dt1.Rows[i][j]}' vs '{dt2.Rows[i][j]}'");
                    return false;
                }
            }
        }
        return true;
    }

    //public class DataMismatch
    //{
    //    public int RowIndex { get; set; }
    //    public string ColumnName { get; set; }
    //    public object OldValue { get; set; }
    //    public object NewValue { get; set; }
    //}

    //public List<DataMismatch> FindMismatches(DataTable dt1, DataTable dt2)
    //{
    //    List<DataMismatch> mismatches = new List<DataMismatch>();
    //    if (dt1.Rows.Count != dt2.Rows.Count)
    //    {
    //        Log.Information($"Row count mismatch: {dt1.Rows.Count} in SQL Server vs {dt2.Rows.Count} in Access");
    //        // Consider handling row count differences if needed
    //    }
    //    for (int i = 0; i < Math.Min(dt1.Rows.Count, dt2.Rows.Count); i++)
    //    {
    //        foreach (DataColumn column in dt1.Columns)
    //        {
    //            if (!Equals(dt1.Rows[i][column], dt2.Rows[i][column]))
    //            {
    //                mismatches.Add(new DataMismatch
    //                {
    //                    RowIndex = i,
    //                    ColumnName = column.ColumnName,
    //                    OldValue = dt1.Rows[i][column],
    //                    NewValue = dt2.Rows[i][column]
    //                });
    //            }
    //        }
    //    }
    //    return mismatches;
    //}








    // This was added to allow the connection string to be changed after the object is created
    // from a standard connection string to read only. However, that distinction didn't make much of a difference
    // during data handling. Leaving it here for future use.
    public string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_connectionString != value)
            {
                _connectionString = value;
                // Optionally add logic to handle connection changes, like reinitializing connections
            }
        }
    }

}
