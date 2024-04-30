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
    static bool syncActive = false; // Flag to control the sync process
    static Task syncTask; // Task to run the synchronization process
    static string verifyTableName = "tblPrinters"; // Table to verify data consistency

    static void Main(string[] args)
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            //.WriteTo.Console()
            .WriteTo.File("C:\\Users\\eaugusto\\Desktop\\Logs\\dataSyncLog.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();

        // Setup Dependency Injection
        var serviceProvider = new ServiceCollection()
            .AddMemoryCache()
            .BuildServiceProvider();

        var cache = serviceProvider.GetService<IMemoryCache>();
        var tableNames = new List<string>();  // List to keep track of table names loaded into memory

        // Define connection strings for databases
        var accessConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eaugusto\\Desktop\\SummitBE_local.accdb;";
        var sqlConnectionString = "Server=.\\summitlocal;Database=summitlocal;User Id=sa;Password=CTK1420!@;";

        // Declare and instantiate schema managers here
        var accessSchemaManager = new AccessDatabaseSchemaManager(accessConnectionString);
        var sqlSchemaManager = new SqlServerSchemaManager(sqlConnectionString);

        var accessLoader = new AccessDataLoader(accessConnectionString, cache, tableNames);
        var sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, tableNames);

        bool running = true;
        Log.Information("Starting application");

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
