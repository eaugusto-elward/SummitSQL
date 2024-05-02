using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Data;
using SummitSQL;
using Serilog;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Quartz.Impl.AdoJobStore.Common;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.IO;

class Program
{
    static bool syncActive = false; // Flag to check if data synchronization is currently active
    static Task? syncTask;  // Task that runs the data synchronization process
    static string verifyTableName = "tblPerson";  // Default table name used to verify data consistency between databases
    static CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(); // Source of cancellation tokens that can be used to stop the synchronization process
    static IMemoryCache cache = new MemoryCache(new MemoryCacheOptions());  // Cache for storing persistent data
    static IMemoryCache volatileCache = new MemoryCache(new MemoryCacheOptions());  // Cache for storing volatile data that updates frequently

    static void Main(string[] args)
    {
        // Initialize and configure the logger
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.File("C:\\Users\\eaugusto\\Desktop\\Logs\\dataSyncLog.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();

        // List of table names to keep track of those that have been loaded into memory
        var tableNames = new List<string>();

        // Connection strings for the Access and SQL Server databases
        var accessConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eaugusto\\Desktop\\SummitBE_local.accdb;Mode=Read";
        //var accessConnectionStringReadOnly = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eaugusto\\Desktop\\SummitBE_local.accdb;Mode = Read; ";
        var sqlConnectionString = "Server=.\\summitlocal;Database=summitlocal;User Id=sa;Password=CTK1420!@;";

        // Schema managers for both Access and SQL Server databases to manage database schemas
        var accessSchemaManager = new AccessDatabaseSchemaManager(accessConnectionString);
        var sqlSchemaManager = new SqlServerSchemaManager(sqlConnectionString);

        // Instantiate the AccessDataLoader
        var accessLoader = new AccessDataLoader(accessConnectionString, cache);

        // Now pass the TableNames from accessLoader to SqlServerDataLoader
        var sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, accessLoader.TableNames);


        // Flag to control the main loop of the program
        bool running = true;
        Log.Information("Starting application");

        // Main loop for user interaction
        while (running)
        {
            Console.WriteLine("\nMain Menu:");
            Console.WriteLine("1 - Connect to Access DB");
            Console.WriteLine("2 - Connect to SQL Server");
            Console.WriteLine("3 - Execute SQL to Drop Tables");
            Console.WriteLine("4 - Perform Database Migration");
            Console.WriteLine("5 - Load Access DB into Memory");
            Console.WriteLine("6 - Copy Data to SQL Server");
            Console.WriteLine("7 - Start Data Sync");
            Console.WriteLine("8 - Stop Data Sync");
            Console.WriteLine("9 - Exit");
            Console.WriteLine("10 - Print Cached Data");
            Console.WriteLine("11 - Verify Data Consistency between Access & SQL");
            Console.WriteLine("12 - Clear Cache");
            Console.Write("Enter option: ");

            string? option = Console.ReadLine();
            switch (option)
            {
                case "1":
                    Log.Information("Connecting to Access Database...");
                    Console.WriteLine("Connecting to Access Database...");
                    TestConnection(accessConnectionString, "Access");
                    break;
                case "2":
                    Log.Information("Connecting to SQL Server Database...");
                    Console.WriteLine("Connecting to SQL Server Database...");
                    TestConnection(sqlConnectionString, "SQL Server");
                    break;
                case "3":
                    Log.Information("Dropping all user tables...");
                    Console.WriteLine("Dropping all user tables...");
                    DropAllTables(sqlConnectionString);
                    break;
                case "4":
                    Log.Information("Migrating Database...");
                    Console.WriteLine("Migrating Database...");
                    MigrateDatabase(accessSchemaManager, sqlSchemaManager);
                    break;
                case "5":
                    //accessLoader.ConnectionString = accessConnectionStringReadOnly;
                    Log.Information("Loading Access Database into memory...");
                    Console.WriteLine("Loading Access Database into memory...");
                    accessLoader.LoadAllTablesIntoMemory();
                    //accessLoader.ConnectionString = accessConnectionString;
                    break;
                case "6":
                    Log.Information("Copying data to SQL Server...");
                    Console.WriteLine("Copying data to SQL Server...");
                    sqlLoader.TransferDataToSqlServer();

                    break;
                case "7":
                    if (!syncActive)
                    {
                        Console.WriteLine("Starting data synchronization...");
                        StartDataSync(accessLoader, sqlLoader, tableNames, cache, volatileCache);
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Data synchronization is already running.");
                        Console.ResetColor();
                    }
                    break;
                case "8":
                    Console.WriteLine("Stopping data synchronization...");
                    StopDataSync();
                    break;
                case "9":
                    running = false;
                    StopDataSync(); // Ensure sync is stopped before exiting
                    Console.WriteLine("Exiting program.");
                    ClearCache(cache, volatileCache);
                    break;
                case "10":
                    accessLoader.PrintCachedData();
                    break;
                case "11":
                    sqlLoader.VerifyDataConsistency(verifyTableName);
                    break;
                case "12":
                    ClearCache(cache, volatileCache);
                    break;
                default:
                    Log.Error("Invalid option selected.");
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Invalid option, please try again.");
                    Console.ResetColor();
                    break;
            }


        }
    }


    /// <summary>
    /// Starts the data synchronization process if it's not already running.
    /// </summary>
    /// <param name="accessLoader">Data loader for the Access database.</param>
    /// <param name="sqlLoader">Data loader for the SQL Server.</param>
    /// <param name="tableNames">List of table names to be synchronized.</param>
    /// <param name="cache">Cache service instance.</param>
    /// <param name="volatileCache">Cache for holding the most recent data from Access.</param>
    static void StartDataSync(AccessDataLoader accessLoader, SqlServerDataLoader sqlLoader, List<string> tableNames, IMemoryCache cache, IMemoryCache volatileCache)
    {
        if (!syncActive)
        {
            syncActive = true;
            cancellationTokenSource = new CancellationTokenSource();
            syncTask = Task.Run(() => ContinuousDataCheck(accessLoader, sqlLoader, cache, volatileCache, cancellationTokenSource.Token));
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Data synchronization started.");
            Console.ResetColor();
            Log.Information("Data synchronization started.");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Data synchronization is already running.");
            Console.ResetColor();
            Log.Warning("Data synchronization is already running.");
        }
    }

    /// <summary>
    /// Stops the data synchronization process safely.
    /// </summary>
    static void StopDataSync()
    {
        if (!syncActive) return; // Sync is not active, do nothing
        cancellationTokenSource.Cancel();
        syncActive = false;
        Log.Information("Data synchronization stopped.");
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("Data synchronization stopped.");
        Console.ResetColor();
    }

    /// <summary>
    /// Continuously checks data for updates and synchronizes changes using two caches.
    /// </summary>
    /// <param name="accessLoader">Data loader for the Access database.</param>
    /// <param name="sqlLoader">Data loader for the SQL Server.</param>
    /// <param name="tableNames">List of table names to check.</param>
    /// <param name="primaryCache">Cache service instance for persistent data.</param>
    /// <param name="volatileCache">Cache for holding the most recent data from Access.</param>
    /// <param name="cancellationToken">Cancellation token to handle stopping the task gracefully.</param>
    static async Task ContinuousDataCheck(AccessDataLoader accessLoader, SqlServerDataLoader sqlLoader, IMemoryCache primaryCache, IMemoryCache volatileCache, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var entry in accessLoader.TableNames)
            {
                var originalName = entry.Key;
                var sanitized = entry.Value;

                try
                {
                    Log.Information($"Checking updates for {originalName}");
                    DataTable recentData = accessLoader.LoadTableDataDirectly(originalName);
                    volatileCache.Set(sanitized, recentData, new MemoryCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(5) });

                    if (primaryCache.TryGetValue(sanitized, out DataTable? primaryData))
                    {
                        if (primaryData != null && recentData != null)
                        {
                            if (!accessLoader.TablesMatch(primaryData, recentData))
                            {
                                //var mismatches = accessLoader.FindMismatches(primaryData, recentData);
                                //foreach (var mismatch in mismatches)
                                //{
                                //    Log.Information($"Mismatch at row {mismatch.RowIndex} in {sanitized}, column {mismatch.ColumnName}: Cached='{mismatch.OldValue}', New='{mismatch.NewValue}'");
                                //}

                                Log.Information($"Data mismatch found for {sanitized}, updating SQL Server.");
                                sqlLoader.UpdateSqlServer(sanitized, recentData);
                                primaryCache.Set(sanitized, recentData);
                            }
                        }
                    }
                    else
                    {
                        // Log that the table was correctly compared between the two caches and no changes were found
                        primaryCache.Set(sanitized, recentData);
                        Log.Information($"Data for {sanitized} is up to date.");
                    }
                    // Task delay used in place of event trigger, keeps async stack from growing too large and causing memory leak on exit
                    await Task.Delay(1, cancellationToken); // Consider making the delay configurable
                }
                catch (Exception ex)
                {
                    Log.Error($"Error processing table {originalName}: {ex.Message}");
                }
            }
        }
    }

    /// <summary>
    /// Clears all data stored the primary and secondary caches.
    /// </summary>
    /// <param name="primaryCache">Primary memory cache to be cleared.</param>
    /// <param name="volatileCache">Volatile memory cache to be cleared.</param>
    static void ClearCache(IMemoryCache primaryCache, IMemoryCache volatileCache)
    {
        if (cache is MemoryCache memCache && volatileCache is MemoryCache volatileMemCache)
        {
            memCache.Compact(1.0); // This effectively clears the cache
            volatileMemCache.Compact(1.0);
            Log.Information("Cache has been cleared.");
            Console.WriteLine("Cache cleared.");
        }
    }

    /// <summary>
    /// Tests the connection to the specified database type and logs the result.
    /// </summary>
    /// <param name="connectionString">Connection string for the database.</param>
    /// <param name="dbType">Type of the database (Access or SQL Server).</param>
    static void TestConnection(string connectionString, string dbType)
    {
        try
        {
            if (dbType == "Access" && OperatingSystem.IsWindows())
            {
                // Use OleDbConnection for Access Database
                using (var connection = new System.Data.OleDb.OleDbConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Log.Information($"{dbType} connection successful.");
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{dbType} connection successful.");
                        Console.ResetColor();
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"Error connecting to {dbType}: {ex.Message}");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error connecting to {dbType}: {ex.Message}");
                        Console.ResetColor();
                    }
                }
            }
            else if (dbType == "SQL Server")
            {
                // Use SqlConnection for SQL Server Database
                using (var connection = new System.Data.SqlClient.SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Log.Information($"{dbType} connection successful.");
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{dbType} connection successful.");
                        Console.ResetColor();
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"Error connecting to {dbType}: {ex.Message}");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error connecting to {dbType}: {ex.Message}");
                        Console.ResetColor();
                    }
                }
            }
            else
            {
                Log.Error($"Unsupported database type: {dbType}");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Unsupported database type: {dbType}");
                Console.ResetColor();
            }
        }
        catch (Exception ex)
        {
            Log.Error($"Error testing connection: {ex.Message}");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error testing connection: {ex.Message}");
            Console.ResetColor();
        }
    }

    /// <summary>
    /// Drops all user tables from the SQL Server database.
    /// </summary>
    /// <param name="connectionString">SQL Server database connection string.</param>
    static void DropAllTables(string connectionString)
    {
        string query = @"
            DECLARE @TableName NVARCHAR(MAX);
            DECLARE @DropTableSQL NVARCHAR(MAX);

            DECLARE table_cursor CURSOR FOR
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE';

            OPEN table_cursor;

            FETCH NEXT FROM table_cursor INTO @TableName;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @DropTableSQL = 'DROP TABLE ' + QUOTENAME(@TableName);
                EXEC sp_executesql @DropTableSQL;
                FETCH NEXT FROM table_cursor INTO @TableName;
            END;

            CLOSE table_cursor;
            DEALLOCATE table_cursor;";

        using (var connection = new System.Data.SqlClient.SqlConnection(connectionString))
        {
            connection.Open();
            var command = new System.Data.SqlClient.SqlCommand(query, connection);
            try
            {
                command.ExecuteNonQuery();
                Log.Information("All tables dropped successfully.");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("All tables dropped successfully.");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Log.Error($"Error dropping tables: {ex.Message}");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error dropping tables: {ex.Message}");
                Console.ResetColor();
            }
        }
    }

    /// <summary>
    /// Migrates database schema from Access to SQL Server.
    /// </summary>
    /// <param name="accessSchemaManager">Schema manager for Access database.</param>
    /// <param name="sqlSchemaManager">Schema manager for SQL Server.</param>
    static void MigrateDatabase(AccessDatabaseSchemaManager accessSchemaManager, SqlServerSchemaManager sqlSchemaManager)
    {
        DataTable accessTables = accessSchemaManager.GetAccessTables();
        foreach (DataRow table in accessTables.Rows)
        {
            string? tableName = table["TABLE_NAME"].ToString();
            if (tableName != null && !tableName.StartsWith("MSys")) // Exclude system tables
            {
                DataTable columns = accessSchemaManager.GetTableColumns(tableName);
                sqlSchemaManager.CreateTable(columns, tableName);
            }
        }
        Log.Information("Database migration complete.");
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("Database migration complete.");
        Console.ResetColor();
    }
}
