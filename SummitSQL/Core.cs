using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;

/// <summary>
/// Core controller for managing data operations from Access to SQL Server.
/// </summary>
public class Core
{
    private AccessDataLoader _accessLoader;
    private SqlServerDataLoader _sqlLoader;

    /// <summary>
    /// Initializes a new instance of the Core class with specified services.
    /// </summary>
    /// <param name="accessConnectionString">Access database connection string.</param>
    /// <param name="sqlConnectionString">SQL Server connection string.</param>
    /// <param name="cache">Memory cache to store Access data temporarily.</param>
    /// <param name="tableNames">List to track table names for synchronization.</param>
    public Core(string accessConnectionString, string sqlConnectionString, IMemoryCache cache, List<string> tableNames)
    {
        _accessLoader = new AccessDataLoader(accessConnectionString, cache, tableNames);
        _sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, tableNames);
    }

    /// <summary>
    /// Executes the full data loading from Access and transferring to SQL Server.
    /// </summary>
    public void ExecuteDataOperations()
    {
        _accessLoader.LoadAllTablesIntoMemory();
        _sqlLoader.TransferDataToSqlServer();
    }
}
