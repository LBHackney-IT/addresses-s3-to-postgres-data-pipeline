using System;
using AddressesDataPipeline.Database;
using Amazon.Lambda.Core;
using FluentAssertions;
using Moq;
using Npgsql;
using NUnit.Framework;

namespace AddressesDataPipeline.Tests
{
    public class DatabaseActionsTests : DatabaseTests
    {
        private DatabaseActions _databaseActions;
        private Mock<ILambdaContext> _contextMock;

        [SetUp]
        public void SetUp()
        {
            _contextMock = new Mock<ILambdaContext>();
            _databaseActions = new DatabaseActions();
            _databaseActions.SetupDatabase(_contextMock.Object);
        }
        [Test]
        public void CanSetupDatabaseConnection()
        {
            DbConnection.Should().NotBeNull();
            DbConnection.Should().BeOfType<NpgsqlConnection>();
        }

        [Test]
        public void CanTruncateTable()
        {
            //create and insert data to test against
            var npgsqlCommand = DbConnection.CreateCommand();
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
            var tableName = "testtablecreate";
            _databaseActions.CreateTable(_contextMock.Object, tableName);
            TableExists(tableName).Should().BeTrue();
        }

        private bool TableExists(string table)
        {
            var npgsqlCommand = DbConnection.CreateCommand();
            npgsqlCommand.CommandText = $@"SELECT EXISTS(SELECT FROM pg_catalog.pg_class c
                                       JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                       WHERE c.relname = '{table}'
                                       );";

            var result = npgsqlCommand.ExecuteScalar();

            return Convert.ToBoolean(result);
        }

        //TODO test for inserting data into Postgres

    }
}