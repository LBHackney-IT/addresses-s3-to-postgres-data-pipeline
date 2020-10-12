using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace S3ToPostgresDataPipeline.Database
{
    public class DatabaseActions : IDatabaseActions
    {
        private NpgsqlConnection _npgsqlConnection;

        public int CopyDataToDatabase(ILambdaContext context, string awsRegion, string bucketName, string objectKey)
        {
            var loadDataCommand = _npgsqlConnection.CreateCommand();

            var loadDataFromCSV = @"SELECT aws_s3.table_import_from_s3('qlik_vulnerability','','(FORMAT csv, HEADER)',@bucket, @objectkey, @awsregion);";
            loadDataCommand.CommandText = loadDataFromCSV;
            loadDataCommand.Parameters.AddWithValue("bucket", bucketName);
            loadDataCommand.Parameters.AddWithValue("objectkey", objectKey);
            loadDataCommand.Parameters.AddWithValue("awsregion", awsRegion);
            var rowsAffected = loadDataCommand.ExecuteNonQuery();
            if (rowsAffected == 0)
            {
                //no insert has occured
                LambdaLogger.Log($"Load has failed - no rows were affected. Ensure the file contained data");
                throw new NpgsqlException($"Load has failed - no rows were loaded from file {bucketName}/{objectKey} in region {awsRegion}");
            }
            return rowsAffected;
        }

        public int TruncateTable(ILambdaContext context,string tableName)
        {
            var npgsqlCommand = _npgsqlConnection.CreateCommand();             
            LambdaLogger.Log($"Table name to truncate {tableName}");
            //TODO improve security in below line
            var truncateTableQuery =$"TRUNCATE TABLE {tableName};";
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
                $"Database={Environment.GetEnvironmentVariable("DB_DATABASE") ?? "s3-to-postgres-data-pipeline-test-db"}" + ";CommandTimeout=120;";
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
    }
}
