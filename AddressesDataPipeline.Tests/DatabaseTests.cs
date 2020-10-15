using System;
using Amazon.Lambda.Core;
using Moq;
using Npgsql;
using NUnit.Framework;

namespace AddressesDataPipeline.Tests
{
    [TestFixture]
    public class DatabaseTests
    {
        protected Mock<ILambdaContext> ContextMock { private set; get; }
        protected NpgsqlConnection DbConnection { private set; get; }

        [SetUp]
        public void Setup()
        {
            ContextMock = new Mock<ILambdaContext>();
            DbConnection = new NpgsqlConnection(GetConnectionString());
            DbConnection.Open();
        }

        [TearDown]
        public void Teardown()
        {
            var npgsqlCommand = DbConnection.CreateCommand();
            npgsqlCommand.CommandText = "DROP TABLE IF EXISTS test;" +
                                        "DELETE FROM dbo.hackney_address;" +
                                        "DELETE FROM dbo.national_address;" +
                                        "DELETE from dbo.address_base;";
            npgsqlCommand.ExecuteNonQuery();
            DbConnection.Close();
            DbConnection.Dispose();
        }

        private static string GetConnectionString()
        {
            return $"Host={Environment.GetEnvironmentVariable("DB_HOST") ?? "127.0.0.1"};" +
                   $"Port={Environment.GetEnvironmentVariable("DB_PORT") ?? "5432"};" +
                   $"Username={Environment.GetEnvironmentVariable("DB_USERNAME") ?? "postgres"};" +
                   $"Password={Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "password"};" +
                   $"Database={Environment.GetEnvironmentVariable("DB_DATABASE") ?? "address-to-postgres-data-pipeline-test-db"}" + ";CommandTimeout=120;";
        }

        protected void CreateTable(string tableName)
        {
            var npgsqlCommand = DbConnection.CreateCommand();
            npgsqlCommand.CommandText = $"CREATE TABLE IF NOT EXISTS {tableName} (addresskey int);";
            npgsqlCommand.ExecuteNonQuery();
            npgsqlCommand.CommandText = @"DROP TABLE IF EXISTS test2;";
            npgsqlCommand.ExecuteNonQuery();
        }

        protected long CountRows(string tableName = "test")
        {
            var npgsqlCommand = DbConnection.CreateCommand();
            npgsqlCommand.CommandText = $"SELECT COUNT(*) FROM {tableName};";

            var result = npgsqlCommand.ExecuteScalar();

            return Convert.ToInt64(result);
        }
    }
}
