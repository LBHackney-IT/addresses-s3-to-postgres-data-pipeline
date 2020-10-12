using Amazon.Lambda.Core;
using FluentAssertions;
using Moq;
using Npgsql;
using NUnit.Framework;
using S3ToPostgresDataPipeline.Database;

namespace S3ToPostgresDataPipeline.Tests
{
    [TestFixture]
    public class DatabaseTests
    {
        [Test]
        public void CanTruncateTable()
        {
            var contextMock = new Mock<ILambdaContext>();

            var databaseActions = new DatabaseActions();
            var dbConnection = databaseActions.SetupDatabase(contextMock.Object);
           
            //create and insert data to test against
            var npgsqlCommand = dbConnection.CreateCommand();
            var truncateTableQuery = @"CREATE TABLE test (id, int);";
            npgsqlCommand.CommandText = truncateTableQuery;
            npgsqlCommand.ExecuteNonQuery();

            npgsqlCommand.CommandText = @"INSERT INTO test values (id, 1)";
            npgsqlCommand.ExecuteNonQuery();

            var result = databaseActions.TruncateTable(contextMock.Object, "test");

            result.Should().Be(1);
        }

        [Test]
        public void CanSetupDatabaseConnection()
        {
            var contextMock = new Mock<ILambdaContext>();
            var databaseActions = new DatabaseActions();

            var result = databaseActions.SetupDatabase(contextMock.Object);

            result.Should().NotBeNull();
            result.Should().BeOfType<NpgsqlConnection>();
        }

        //TODO test for inserting data into Postgres
    }
}
