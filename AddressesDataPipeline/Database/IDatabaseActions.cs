using Amazon.Lambda.Core;
using Npgsql;

namespace AddressesDataPipeline.Database
{
    public interface IDatabaseActions
    {
        int TruncateTable(ILambdaContext context, string tableName);
        int CopyDataToDatabase(string tableName, ILambdaContext context, string awsRegion, string bucketName, string objectKey);
        void CreateTable(ILambdaContext context, string tableName);
        void AddExtension(ILambdaContext context);
        NpgsqlConnection SetupDatabase(ILambdaContext context);
        public int TransformDataAndInsert(string tableName, string cursor, int? limit, string gazetteer);
    }
}
