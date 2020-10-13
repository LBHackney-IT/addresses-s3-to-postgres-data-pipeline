using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AddressesDataPipeline.Database
{
    public class DatabaseActions : IDatabaseActions
    {
        private NpgsqlConnection _npgsqlConnection;

        public int CopyDataToDatabase(string tableName, ILambdaContext context, string awsRegion, string bucketName, string objectKey)
        {
            var loadDataCommand = _npgsqlConnection.CreateCommand();

            var loadDataFromCSV = @"SELECT aws_s3.table_import_from_s3(@tablename,'','(FORMAT csv, HEADER)',@bucket, @objectkey, @awsregion);";
            loadDataCommand.CommandText = loadDataFromCSV;
            loadDataCommand.Parameters.AddWithValue("bucket", bucketName);
            loadDataCommand.Parameters.AddWithValue("objectkey", objectKey);
            loadDataCommand.Parameters.AddWithValue("awsregion", awsRegion);
            loadDataCommand.Parameters.AddWithValue("tablename", tableName);
            var rowsAffected = loadDataCommand.ExecuteNonQuery();
            if (rowsAffected == 0)
            {
                //no insert has occured
                LambdaLogger.Log($"Load has failed - no rows were affected. Ensure the file contained data");
                throw new NpgsqlException($"Load has failed - no rows were loaded from file {bucketName}/{objectKey} in region {awsRegion}");
            }
            return rowsAffected;
        }

        public int TruncateTable(ILambdaContext context, string tableName)
        {
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            LambdaLogger.Log($"Table name to truncate {tableName}");
            //TODO improve security in below line
            var truncateTableQuery = $"TRUNCATE TABLE {tableName};";
            npgsqlCommand.CommandText = truncateTableQuery;
            var rowsAffected = npgsqlCommand.ExecuteNonQuery();

            return rowsAffected;
        }
        public NpgsqlConnection SetupDatabase(ILambdaContext context)
        {
            LambdaLogger.Log("set up DB");
            var connString = $"Host={Environment.GetEnvironmentVariable("DB_HOST") ?? "127.0.0.1"};" +
                $"Port={Environment.GetEnvironmentVariable("DB_PORT") ?? "5432"};" +
                $"Username={Environment.GetEnvironmentVariable("DB_USERNAME") ?? "postgres"};" +
                $"Password={Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "password"};" +
                $"Database={Environment.GetEnvironmentVariable("DB_DATABASE") ?? "address-to-postgres-data-pipeline-test-db"}" + ";CommandTimeout=120;";
            try
            {
                var connection = new NpgsqlConnection(connString);
                LambdaLogger.Log("Opening DB connection");
                connection.Open();
                _npgsqlConnection = connection;
                return connection;
            }
            catch (Exception ex)
            {
                LambdaLogger.Log($"Exception has occurred while setting up DB connection - {ex.Message} {ex.InnerException} {ex.StackTrace}");
                throw ex;
            }
        }

        public void CreateTable(ILambdaContext context, string tableName)
        {
            LambdaLogger.Log("Create table if it doesn't exist");
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            var createTableQuery = $"CREATE TABLE IF NOT EXISTS {tableName} {GetCreateTableScript()};";
            npgsqlCommand.CommandText = createTableQuery;
            npgsqlCommand.ExecuteNonQuery();
        }

        public string GetCreateTableScript()
        {
            var createTableSql = @"(uprn double precision NOT NULL,
                                        parent_uprn double precision,
                                        udprn double precision,
                                        usrn double precision NOT NULL,
                                        toid character varying(20),
                                        classification_code character varying(6),
                                        easting numeric(12, 4),
                                        northing numeric(12, 4),
                                        latitude numeric(12, 9),
                                        longitude numeric(12, 9),
                                        rpc character varying(10),
                                        last_update_date date,
                                        single_line_address character varying(800),
                                        po_box character varying(18),
                                        organisation character varying(100),
                                        sub_building character varying(120),
                                        building_name character varying(100),
                                        building_number character varying(17),
                                        street_name character varying(100),
                                        locality character varying(100),
                                        town_name character varying(100),
                                        post_town character varying(100),
                                        island character varying(8),
                                        postcode character varying(8),
                                        delivery_point_suffix character varying(8),
                                        gss_code character varying(100),
                                        change_code character varying(8)
                                    )";
            return createTableSql;
        }

        public void AddExtension(ILambdaContext context)
        {
            LambdaLogger.Log("Add aws_s3 extension to database");
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            var addExtensionQuery = $"CREATE EXTENSION aws_s3 CASCADE;";
            npgsqlCommand.CommandText = addExtensionQuery;
            npgsqlCommand.ExecuteNonQuery();
        }
    }
}
