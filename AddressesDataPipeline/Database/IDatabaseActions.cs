﻿using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AddressesDataPipeline.Database
{
    public interface IDatabaseActions
    {
        int TruncateTable(ILambdaContext context, string tableName);
        int CopyDataToDatabase(string tableName, ILambdaContext context, string awsRegion, string bucketName, string objectKey);
        void CreateTable(ILambdaContext context, string tableName);
        void AddExtension(ILambdaContext context);
        NpgsqlConnection SetupDatabase(ILambdaContext context);
    }
}
