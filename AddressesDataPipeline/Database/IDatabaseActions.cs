using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AddressDataPipeline.Database
{
    public interface IDatabaseActions
    {
        int TruncateTable(ILambdaContext context,string tableName);
        int CopyDataToDatabase(string tableName, ILambdaContext context,string awsRegion, string bucketName, string objectKey);
        NpgsqlConnection SetupDatabase(ILambdaContext context);
    }
}
