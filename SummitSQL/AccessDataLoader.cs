using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;

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

}
