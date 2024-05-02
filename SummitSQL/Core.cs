using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using Serilog;

/// <summary>
/// Core controller for managing data operations from an Access database to a SQL Server database.
/// This class orchestrates the loading of data from Access into memory and then transferring that data to SQL Server.
/// </summary>
public class Core
{
    private AccessDataLoader _accessLoader;
    private SqlServerDataLoader _sqlLoader;
    //private Dictionary<string, string> _tableNames; // Shared dictionary for table names

    /// <summary>
    /// Initializes a new instance of the Core class with specified services.
    /// </summary>
    /// <param name="accessConnectionString">Access database connection string.</param>
    /// <param name="sqlConnectionString">SQL Server database connection string.</param>
    /// <param name="cache">Memory cache to store Access data temporarily.</param>
    public Core(string accessConnectionString, string sqlConnectionString, IMemoryCache cache)
    {
        var tableNames = new Dictionary<string, string>(); // Shared dictionary
        _accessLoader = new AccessDataLoader(accessConnectionString, cache);
        _sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, tableNames);
    }

    /// <summary>
    /// Executes the full data loading from Access and transferring to SQL Server.
    /// This method is designed to be called to perform the entire process of data migration from Access to SQL Server.
    /// </summary>
    public void ExecuteDataOperations()
    {
        Log.Information("Starting full data load and transfer operations.");
        _accessLoader.LoadAllTablesIntoMemory();
        _sqlLoader.TransferDataToSqlServer();
        Log.Information("Data operations completed.");
    }
}
