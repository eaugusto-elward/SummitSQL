using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;

/// <summary>
/// Main program class to setup services and execute data synchronization.
/// </summary>
class Program
{
    static void Main(string[] args)
    {
        // Setup Dependency Injection
        var serviceProvider = new ServiceCollection()
            .AddMemoryCache()
            .BuildServiceProvider();

        var cache = serviceProvider.GetService<IMemoryCache>();
        var tableNames = new List<string>();  // List to keep track of table names loaded into memory

        // Define connection strings for databases
        var accessConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eaugusto\\Desktop\\SummitBE_local.accdb;";
        var sqlConnectionString = "Server=WS-502\\summitlocal;Database=summitlocal;User Id=sa;Password=CTK1420!@;";

        // Initialize and run core synchronization operations
        var core = new Core(accessConnectionString, sqlConnectionString, cache, tableNames);
        core.ExecuteDataOperations();

        Console.WriteLine("Data operations completed.");
    }
}
