using System;
using Amazon.Lambda.Core;
using FluentAssertions;
using Moq;
using Npgsql;
using NUnit.Framework;
using AddressesDataPipeline.Database;

namespace AddressesDataPipeline.Tests
{
    [TestFixture]
    public class DatabaseTests
    {
        private Mock<ILambdaContext> _contextMock;
        private DatabaseActions _databaseActions;
        private NpgsqlConnection _dbConnection;

        [SetUp]
        public void Setup()
        {
            _contextMock = new Mock<ILambdaContext>();
            _databaseActions = new DatabaseActions();
            _dbConnection = _databaseActions.SetupDatabase(_contextMock.Object);
        }

        [Test]
        public void CanSetupDatabaseConnection()
        {
            _dbConnection.Should().NotBeNull();
            _dbConnection.Should().BeOfType<NpgsqlConnection>();
        }

        [Test]
        public void CanTruncateTable()
        {
            //create and insert data to test against
            var npgsqlCommand = _dbConnection.CreateCommand();
            npgsqlCommand.CommandText = @"CREATE TABLE IF NOT EXISTS test (id int);";
            npgsqlCommand.ExecuteNonQuery();

            npgsqlCommand.CommandText = @"INSERT INTO test values (1);";
            npgsqlCommand.ExecuteNonQuery();

            CountRows().Should().Be(1);

            _databaseActions.TruncateTable(_contextMock.Object, "test");
            CountRows().Should().Be(0);
        }

        [Test]
        public void CanCreateTable()
        {
            _databaseActions.CreateTable(_contextMock.Object, "dbo.test2");
            TableExists("dbo", "test2").Should().BeTrue();
        }

        [TearDown]
        public void Teardown()
        {
            var npgsqlCommand = _dbConnection.CreateCommand();
            npgsqlCommand.CommandText = @"DROP TABLE IF EXISTS test;";
            npgsqlCommand.ExecuteNonQuery();
            npgsqlCommand.CommandText = @"DROP TABLE IF EXISTS test2;";
            npgsqlCommand.ExecuteNonQuery();
        }

        private long CountRows()
        {
            var npgsqlCommand = _dbConnection.CreateCommand();
            npgsqlCommand.CommandText = @"SELECT COUNT(*) FROM test;";

            var result = npgsqlCommand.ExecuteScalar();

            return Convert.ToInt64(result);
        }

        private bool TableExists(string schema, string table)
        {
            var npgsqlCommand = _dbConnection.CreateCommand();
            npgsqlCommand.CommandText = $@"SELECT EXISTS(SELECT FROM pg_catalog.pg_class c
                                       JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                       WHERE n.nspname = '{schema}'
                                       AND c.relname = '{table}'
                                       );";

            var result = npgsqlCommand.ExecuteScalar();

            return Convert.ToBoolean(result);
        }

        //TODO test for inserting data into Postgres
    }
}
