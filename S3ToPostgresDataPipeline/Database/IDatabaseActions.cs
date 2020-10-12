using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace S3ToPostgresDataPipeline.Database
{
    public interface IDatabaseActions
    {
        int TruncateTable(ILambdaContext context,string tableName);
        int CopyDataToDatabase(ILambdaContext context,string awsRegion, string bucketName, string objectKey);
        NpgsqlConnection SetupDatabase(ILambdaContext context);
    }
}
