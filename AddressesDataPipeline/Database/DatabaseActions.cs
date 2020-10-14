using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.Lambda.Core;
using Dapper;
using Npgsql;

namespace AddressesDataPipeline.Database
{
    public class DatabaseActions : IDatabaseActions
    {
        private NpgsqlConnection _npgsqlConnection;
        private const string _hackneyGssCode = "E09000012";

        public int CopyDataToDatabase(string tableName, ILambdaContext context, string awsRegion, string bucketName, string objectKey)
        {
            var loadDataCommand = _npgsqlConnection.CreateCommand();
            LambdaLogger.Log($"Copying data to table ({tableName}) from region ({awsRegion}) and bucket ({bucketName}) and file ({objectKey})");
            var loadDataFromCSV = @"SELECT aws_s3.table_import_from_s3(@tablename,'','(FORMAT csv, HEADER)',@bucket, @objectkey, @awsregion);";
            loadDataCommand.CommandText = loadDataFromCSV;
            loadDataCommand.Parameters.AddWithValue("bucket", bucketName);
            loadDataCommand.Parameters.AddWithValue("objectkey", objectKey);
            loadDataCommand.Parameters.AddWithValue("awsregion", awsRegion);
            loadDataCommand.Parameters.AddWithValue("tablename", tableName);
            var rowsAffected = loadDataCommand.ExecuteNonQuery();
            if (rowsAffected == 0)
            {
                //no insert has occured
                LambdaLogger.Log($"Load has failed - no rows were affected. Ensure the file contained data");
                throw new NpgsqlException($"Load has failed - no rows were loaded from file {bucketName}/{objectKey} in region {awsRegion}");
            }
            _npgsqlConnection.Close();
            return rowsAffected;
        }

        public int TransformDataAndInsert(string tableName, string cursor, int? limit, string gazetteer)
        {
            var records = GetRecordsFromAddressBase(cursor, limit, gazetteer);
            var databaseToInsertInto = gazetteer == "local"
                ? "hackney_address"
                : "national_address";
            var insertStatement = $"INSERT INTO dbo.{databaseToInsertInto}" +
              "(lpi_key,uprn,usrn,parent_uprn,lpi_logical_status,sao_text,pao_text,building_number,street_description," +
              "postcode,postcode_nospace,locality,gazetteer,organisation,ward,usage_description,usage_primary,blpu_class," +
              "planning_use_class,property_shell,neverexport,easting,northing,longitude," +
              "latitude,lpi_start_date,lpi_end_date,lpi_last_update_date,blpu_start_date,blpu_end_date,blpu_last_update_date," +
              "line1,line2,line3,line4,town)" +
              "VALUES ";

            var values = string.Join(", ", records.Select(x =>
                $"('{NumericalIdToUniqueString(x.id)}' ,{x.uprn}, {x.usrn}, {x.parent_uprn}, 'Approved Preferred', '{x.sub_building}'," +
                $" '{x.building_name}', '{x.building_number}', '{x.street_name}', '{x.postcode}', '{x.postcode.Replace(" ", "")}', " +
                $" '{x.locality}', '{gazetteer}', " +
                $"'{x.organisation}', '','{GetUsageDescription(x.classification_code)}', '{GetUsageDescription(x.classification_code)}', " +
                $"'{x.classification_code.Trim().Substring(0, 4)}', '',{x.classification_code.First() == 'P'}, false, {x.easting}, {x.northing}, " +
                $"{x.longitude}, {x.latitude}, 0, 0, 0, 0, 0, 0, {GetAddressLines(x.single_line_address)}, '{x.town_name}')"));
            return _npgsqlConnection.Execute(insertStatement + values);
        }

        public int TruncateTable(ILambdaContext context, string tableName)
        {
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            LambdaLogger.Log($"Table name to truncate {tableName}");
            //TODO improve security in below line
            var truncateTableQuery = $"TRUNCATE TABLE {tableName};";
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
                $"Database={Environment.GetEnvironmentVariable("DB_DATABASE") ?? "address-to-postgres-data-pipeline-test-db"}" + ";CommandTimeout=120;";
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

        public void CreateTable(ILambdaContext context, string tableName)
        {
            LambdaLogger.Log("Create table if it doesn't exist");
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            var createTableQuery = $"CREATE TABLE IF NOT EXISTS {tableName} {GetCreateTableScript()};";
            npgsqlCommand.CommandText = createTableQuery;
            npgsqlCommand.ExecuteNonQuery();
        }

        public void AddExtension(ILambdaContext context)
        {
            LambdaLogger.Log("Add aws_s3 extension to database");
            var npgsqlCommand = _npgsqlConnection.CreateCommand();
            var addExtensionQuery = $"CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;";
            npgsqlCommand.CommandText = addExtensionQuery;
            npgsqlCommand.ExecuteNonQuery();
        }

        private static string GetCreateTableScript()
        {
            var createTableSql = @"(uprn double precision NOT NULL,
                                        parent_uprn double precision,
                                        udprn double precision,
                                        usrn double precision NOT NULL,
                                        toid character varying(20),
                                        classification_code character varying(6),
                                        easting numeric(12, 4),
                                        northing numeric(12, 4),
                                        latitude numeric(12, 9),
                                        longitude numeric(12, 9),
                                        rpc character varying(10),
                                        last_update_date date,
                                        single_line_address character varying(800),
                                        po_box character varying(18),
                                        organisation character varying(100),
                                        sub_building character varying(120),
                                        building_name character varying(100),
                                        building_number character varying(17),
                                        street_name character varying(100),
                                        locality character varying(100),
                                        town_name character varying(100),
                                        post_town character varying(100),
                                        island character varying(8),
                                        postcode character varying(8),
                                        delivery_point_suffix character varying(8),
                                        gss_code character varying(100),
                                        change_code character varying(8)
                                    )";
            return createTableSql;
        }

        private IEnumerable<CsvUploadRecord> GetRecordsFromAddressBase(string cursor, int? limit, string gazetteer)
        {
            var onlyIncludeCorrectGazetteer = gazetteer == "local"
                ? $"gss_code = '{_hackneyGssCode}'"
                : $"gss_code != '{_hackneyGssCode}'";
            var numericCursor = Convert.ToInt64(cursor);

            var selectText =
                "SELECT uprn,usrn,parent_uprn,sub_building,building_name,building_number,street_name,postcode,locality,gss_code," +
                "organisation,classification_code,easting,northing,longitude,latitude,single_line_address,town_name, " +
                $"row_number() OVER (PARTITION BY true::boolean) as id FROM dbo.address_base WHERE {onlyIncludeCorrectGazetteer}" +
                " ORDER BY id LIMIT @Limit OFFSET @Cursor;";
            var records = _npgsqlConnection.Query<CsvUploadRecord>(
                selectText, new {Limit = limit, Cursor = numericCursor});
            return records;
        }

        private static string GetAddressLines(string address)
        {
            var addressLines = address.Split(',').Select(line => string.IsNullOrWhiteSpace(line) ? "NULL" : $"'{line}'" ).ToList();
            while (addressLines.Count < 4)
            {
                addressLines.Add("NULL");
            }

            return string.Join(", ", addressLines);
        }

        private static string NumericalIdToUniqueString(long id)
        {
            var numberLength = id.ToString().Length;
            var zeros = "0000000000000";
            var concatted = zeros + id;
            return concatted.Substring(numberLength - 1, 14);
        }

        private static string GetUsageDescription(string code)
        {
            return code.First() switch
            {
                'R' => "Residential",
                'C' => "Commercial",
                'P' => "Parent Shell",
                'L' => "Land",
                'X' => "Dual Use",
                _ => "Unclassified"
            };
        }

    }
}
