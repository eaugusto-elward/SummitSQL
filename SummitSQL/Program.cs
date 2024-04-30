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

class Program
{
    static bool syncActive = false; // Flag to control the synchronization process
    static Task syncTask; // Task to run the data synchronization process
    static string verifyTableName = "tblPrinters"; // Default table to verify data consistency

    static void Main(string[] args)
    {
        // Configure the logger with file output and set the minimum log level to Debug
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.File("C:\\Users\\eaugusto\\Desktop\\Logs\\dataSyncLog.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();

        // Set up dependency injection for memory caching
        var serviceProvider = new ServiceCollection()
            .AddMemoryCache()
            .BuildServiceProvider();

        // Retrieve the memory cache instance from the service provider
        var cache = serviceProvider.GetService<IMemoryCache>();
        var tableNames = new List<string>();  // List to keep track of loaded table names

        // Define connection strings for Access and SQL Server databases
        var accessConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eaugusto\\Desktop\\SummitBE_local.accdb;";
        var sqlConnectionString = "Server=.\\summitlocal;Database=summitlocal;User Id=sa;Password=CTK1420!@;";

        // Instantiate schema managers for both databases
        var accessSchemaManager = new AccessDatabaseSchemaManager(accessConnectionString);
        var sqlSchemaManager = new SqlServerSchemaManager(sqlConnectionString);

        // Create data loaders for both databases
        var accessLoader = new AccessDataLoader(accessConnectionString, cache, tableNames);
        var sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, tableNames);

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
            Console.Write("Enter option: ");

            string option = Console.ReadLine();
            switch (option)
            {
                case "1":
                    Console.WriteLine("Connecting to Access Database...");
                    TestConnection(accessConnectionString, "Access");
                    break;
                case "2":
                    Console.WriteLine("Connecting to SQL Server Database...");
                    TestConnection(sqlConnectionString, "SQL Server");
                    break;
                case "3":
                    Console.WriteLine("Dropping all user tables...");
                    DropAllTables(sqlConnectionString);
                    break;
                case "4":
                    Console.WriteLine("Migrating Database...");
                    MigrateDatabase(accessSchemaManager, sqlSchemaManager);
                    break;
                case "5":
                    Console.WriteLine("Loading Access Database into memory...");
                    accessLoader.LoadAllTablesIntoMemory();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Access Database loaded into memory successfully.");
                    Console.ResetColor();
                    break;
                case "6":
                    Console.WriteLine("Copying data to SQL Server...");
                    sqlLoader.TransferDataToSqlServer();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Data copied to SQL Server successfully.");
                    Console.ResetColor();
                    break;
                case "7":
                    Console.WriteLine("Starting data synchronization...");
                    StartDataSync(accessLoader, sqlLoader, tableNames, cache);
                    break;
                case "8":
                    Console.WriteLine("Stopping data synchronization...");
                    StopDataSync();
                    break;
                case "9":
                    running = false;
                    StopDataSync(); // Ensure sync is stopped before exiting
                    Console.WriteLine("Exiting program.");
                    break;
                case "10":
                    accessLoader.PrintCachedData();
                    break;
                case "11":
                    sqlLoader.VerifyDataConsistency(verifyTableName);
                    break;
                default:
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
    static void StartDataSync(AccessDataLoader accessLoader, SqlServerDataLoader sqlLoader, List<string> tableNames, IMemoryCache cache)
    {
        if (!syncActive)
        {
            syncActive = true;
            syncTask = Task.Run(() => ContinuousDataCheck(accessLoader, sqlLoader, tableNames, cache));
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Data synchronization started.");
            Console.ResetColor();
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Data synchronization is already running.");
            Console.ResetColor();
        }
    }

    /// <summary>
    /// Stops the data synchronization process safely.
    /// </summary>
    static void StopDataSync()
    {
        if (syncActive)
        {
            syncActive = false;
            syncTask?.Wait(); // Safely wait for the task to complete
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Data synchronization stopped.");
            Console.ResetColor();
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Data synchronization is not active.");
            Console.ResetColor();
        }
    }

    /// <summary>
    /// Continuously checks data for updates and synchronizes changes.
    /// </summary>
    /// <param name="accessLoader">Data loader for the Access database.</param>
    /// <param name="sqlLoader">Data loader for the SQL Server.</param>
    /// <param name="tableNames">List of table names to check.</param>
    /// <param name="cache">Cache service instance.</param>
    /// <returns></returns>
    static async Task ContinuousDataCheck(AccessDataLoader accessLoader, SqlServerDataLoader sqlLoader, List<string> tableNames, IMemoryCache cache)
    {
        Process secondConsole = new Process
        {
            StartInfo = new ProcessStartInfo("cmd.exe")
            {
                RedirectStandardInput = true,
                UseShellExecute = false
            }
        };
        secondConsole.Start();
        using (StreamWriter sw = secondConsole.StandardInput)
        {
            if (sw.BaseStream.CanWrite)
            {
                while (syncActive)
                {
                    foreach (var tableName in tableNames)
                    {
                        bool updated = accessLoader.CheckAndUpdateTable(tableName);
                        string status = updated ? "Updated" : "No Change";
                        sw.WriteLine($"Check completed for {tableName}: {status}");
                        if (updated)
                        {
                            if (cache.TryGetValue(tableName, out DataTable updatedTable))
                            {
                                sqlLoader.UpdateSqlServer(tableName, updatedTable);
                            }
                        }
                        await Task.Delay(5000); // Check every 5 seconds
                    }
                }
            }
        }
        secondConsole.WaitForExit();
    }

    /// <summary>
    /// Tests the connection to the specified database type and logs the result.
    /// </summary>
    /// <param name="connectionString">Connection string for the database.</param>
    /// <param name="dbType">Type of the database (Access or SQL Server).</param>
    static void TestConnection(string connectionString, string dbType)
    {
        if (dbType == "Access")
        {
            // Use OleDbConnection for Access Database
            using (var connection = new System.Data.OleDb.OleDbConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"{dbType} connection successful.");
                    Console.ResetColor();
                }
                catch (Exception ex)
                {
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
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"{dbType} connection successful.");
                    Console.ResetColor();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Error connecting to {dbType}: {ex.Message}");
                    Console.ResetColor();
                }
            }
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Unsupported database type: {dbType}");
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
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("All tables dropped successfully.");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
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
            string tableName = table["TABLE_NAME"].ToString();
            if (!tableName.StartsWith("MSys")) // Exclude system tables
            {
                DataTable columns = accessSchemaManager.GetTableColumns(tableName);
                sqlSchemaManager.CreateTable(columns, tableName);
            }
        }
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("Database migration complete.");
        Console.ResetColor();
    }
}
