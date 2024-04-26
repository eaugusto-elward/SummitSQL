using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Data;
using SummitSQL;

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
        var accessConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\eliot\\Desktop\\SummitBE_local.accdb;";
        var sqlConnectionString = "Server=.\\summitlocal;Database=summitlocal;User Id=sa;Password=CTK1420!@;";

        // Declare and instantiate schema managers here
        var accessSchemaManager = new AccessDatabaseSchemaManager(accessConnectionString);
        var sqlSchemaManager = new SqlServerSchemaManager(sqlConnectionString);

        var accessLoader = new AccessDataLoader(accessConnectionString, cache, tableNames);
        var sqlLoader = new SqlServerDataLoader(sqlConnectionString, cache, tableNames);

        bool running = true;
        while (running)
        {
            Console.WriteLine("\nMain Menu:");
            Console.WriteLine("1 - Connect to Access DB");
            Console.WriteLine("2 - Connect to SQL Server");
            Console.WriteLine("3 - Execute SQL to Drop Tables");
            Console.WriteLine("4 - Perform Database Migration");
            Console.WriteLine("5 - Load Access DB into Memory");
            Console.WriteLine("6 - Copy Data to SQL Server");
            Console.WriteLine("7 - Exit");
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
                    Console.WriteLine("Access Database loaded into memory successfully.");
                    break;
                case "6":
                    Console.WriteLine("Copying data to SQL Server...");
                    sqlLoader.TransferDataToSqlServer();
                    Console.WriteLine("Data copied to SQL Server successfully.");
                    break;
                case "7":
                    running = false;
                    Console.WriteLine("Exiting program.");
                    break;
                default:
                    Console.WriteLine("Invalid option, please try again.");
                    break;
            }
        }
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
                    Console.WriteLine($"{dbType} connection successful.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error connecting to {dbType}: {ex.Message}");
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
                    Console.WriteLine($"{dbType} connection successful.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error connecting to {dbType}: {ex.Message}");
                }
            }
        }
        else
        {
            Console.WriteLine($"Unsupported database type: {dbType}");
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
                Console.WriteLine("All tables dropped successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error dropping tables: {ex.Message}");
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
        Console.WriteLine("Database migration complete.");
    }

}
