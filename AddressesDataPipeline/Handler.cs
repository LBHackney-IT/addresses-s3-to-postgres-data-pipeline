using Amazon;
using Amazon.Lambda.Core;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3.Util;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Npgsql;
using AddressesDataPipeline.Database;
using System;
using System.IO;
using System.Linq;
using System.Threading;

// Assembly attribute to enable the Lambda function's JSON input to be
// converted into a .NET class.
[assembly: LambdaSerializer(
    typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AddressesDataPipeline
{
    public static class Program
    {
        public static void Main()
        {
        }
    }

    public class Handler
    {
        private IDatabaseActions _databaseActions;

        public Handler(IDatabaseActions databaseActions)
        {
            _databaseActions = databaseActions;
        }

        public Handler()
        {
            _databaseActions = new DatabaseActions();
        }

        public void LoadCsv(S3EventNotification s3Event, ILambdaContext context)
        {
            LambdaLogger.Log("Processing request started");
            try
            {
                foreach (var record in s3Event.Records)
                {
                    LambdaLogger.Log("Inside of the s3 events loop");
                    var s3 = record.S3;
                    var connection = _databaseActions.SetupDatabase(context);
                    try
                    {
                        string tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
                        //add aws_s3 extension
                        _databaseActions.AddExtension(context);
                        //create table
                        _databaseActions.CreateTable(context, tableName);
                        //truncate correct table
                        _databaseActions.TruncateTable(context, tableName);
                        // load csv data into table
                        _databaseActions.CopyDataToDatabase(tableName, context, record.AwsRegion, s3.Bucket.Name, s3.Object.Key);
                    }
                    catch (NpgsqlException ex)
                    {
                        LambdaLogger.Log($"Npgsql Exception has occurred - {ex.Message} {ex.InnerException} {ex.StackTrace}");
                        throw ex;
                    }
                    //close db connection
                    connection.Close();
                    LambdaLogger.Log("End of function");
                }
            }
            catch (Exception ex)
            {
                LambdaLogger.Log($"Exception has occurred - {ex.Message} {ex.InnerException} {ex.StackTrace}");
                throw ex;
            }
        }

        public void TransformData(TransformDataRequest request, ILambdaContext context)
        {
            var connection = _databaseActions.SetupDatabase(context);
            try
            {
                var tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
                //truncate correct table
                // load csv data into national and/or hackney tables
                _databaseActions.TransformDataAndInsert(tableName, request.cursor, request.limit, request.gazetteer);
            }
            catch (NpgsqlException ex)
            {
                LambdaLogger.Log($"Npgsql Exception has occurred - {ex.Message} {ex.InnerException} {ex.StackTrace}");
                throw ex;
            }
            connection.Close();
        }

        public class TransformDataRequest
        {
            public string cursor { get; set; } = "00000000000000";
            public int? limit { get; set; } = null;

            public string gazetteer { get; set; } = "local";
        }
    }
}
