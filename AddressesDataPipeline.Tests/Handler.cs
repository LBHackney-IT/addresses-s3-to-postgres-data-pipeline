using System;
using System.Collections.Generic;
using System.Linq;
using AddressesDataPipeline.Database;
using Amazon.Lambda.Core;
using Amazon.S3.Util;
using AutoFixture;
using Dapper;
using FluentAssertions;
using Moq;
using Npgsql;
using NUnit.Framework;

namespace AddressesDataPipeline.Tests
{
    [TestFixture]
    public class HandlerTest : DatabaseTests
    {
        private IFixture _fixture = new Fixture();
        private const string _hackneyGssCode = "E09000012";
        [Test]
        public void CanLoadACsvIntoTheDatabase()
        {
            var mockDatabaseActions = new Mock<IDatabaseActions>();
            var handler = new Handler(mockDatabaseActions.Object);
            var tableName = "test";
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", tableName);
            CreateTable("test");

            var bucketData = new S3EventNotification.S3Entity
            {
                Bucket = new S3EventNotification.S3BucketEntity { Name = "testBucket" },
                Object = new S3EventNotification.S3ObjectEntity { Key = "test/key.csv" }
            };

            //S3 record mock
            var testRecord = new S3EventNotification.S3EventNotificationRecord();
            testRecord.AwsRegion = "eu-west-2";
            testRecord.S3 = bucketData;

            var s3EventMock = new S3EventNotification();
            s3EventMock.Records = new List<S3EventNotification.S3EventNotificationRecord> { testRecord };

            var contextMock = new Mock<ILambdaContext>();
            //set up Database actions
            mockDatabaseActions.Setup(x => x.CopyDataToDatabase(tableName, contextMock.Object, testRecord.AwsRegion, bucketData.Bucket.Name, bucketData.Object.Key));
            mockDatabaseActions.Setup(x => x.AddExtension(contextMock.Object));
            mockDatabaseActions.Setup(x => x.CreateTable(contextMock.Object, It.IsAny<string>()));
            mockDatabaseActions.Setup(x => x.TruncateTable(contextMock.Object, It.IsAny<string>()));
            mockDatabaseActions.Setup(x => x.SetupDatabase(contextMock.Object)).Returns(() => new NpgsqlConnection());

            Assert.DoesNotThrow(() => handler.LoadCsv(s3EventMock, contextMock.Object));
            mockDatabaseActions.Verify(y => y.SetupDatabase(contextMock.Object), Times.Once);
            mockDatabaseActions.Verify(y => y.AddExtension(contextMock.Object), Times.Once);
            mockDatabaseActions.Verify(y => y.TruncateTable(contextMock.Object, It.IsAny<string>()), Times.Once);
            mockDatabaseActions.Verify(y => y.CreateTable(contextMock.Object, It.IsAny<string>()), Times.Once);
            mockDatabaseActions.Verify(y => y.CopyDataToDatabase(tableName, contextMock.Object, testRecord.AwsRegion, bucketData.Bucket.Name, bucketData.Object.Key), Times.Once);
        }

        [Test]
        public void TransformsDataAndSaveIntoHackneyAddresses()
        {
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", "dbo.address_base");

            var nationalAddress = CreateRandomAddressBaseRecord("national");
            var hackneyAddress = CreateRandomAddressBaseRecord("local");
            InsertRecordIntoAddressBase(nationalAddress);
            InsertRecordIntoAddressBase(hackneyAddress);

            var handler = new Handler();
            handler.TransformData(new Handler.TransformDataRequest(), new Mock<ILambdaContext>().Object);

            var results = DbConnection.Query<Address>("SELECT * FROM dbo.hackney_address");

            var expectedNationalAddress = MapToExpectedAddressRecord(hackneyAddress);
            results.Count().Should().Be(1);
            results.First().lpi_key.Should().Be("00000000000001");
            results.First().Should().BeEquivalentTo(expectedNationalAddress, options =>
                options.Excluding(x => x.lpi_key).Excluding(x => x.blpu_end_date).Excluding(x => x.blpu_start_date)
                    .Excluding(x => x.blpu_last_update_date).Excluding(x => x.lpi_end_date).Excluding(x => x.lpi_start_date)
                    .Excluding(x => x.lpi_last_update_date));
        }

        [Test]
        public void TransformsDataAndSavesIntoNationalAddresses()
        {
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", "dbo.address_base");

            var nationalAddress = CreateRandomAddressBaseRecord("national");
            var hackneyAddress = CreateRandomAddressBaseRecord("local");
            InsertRecordIntoAddressBase(nationalAddress);
            InsertRecordIntoAddressBase(hackneyAddress);

            var handler = new Handler();
            handler.TransformData(new Handler.TransformDataRequest { gazetteer = "national" }, new Mock<ILambdaContext>().Object);

            var results = DbConnection.Query<Address>("SELECT * FROM dbo.national_address");

            var expectedNationalAddress = MapToExpectedAddressRecord(nationalAddress);
            results.Count().Should().Be(1);
            results.First().lpi_key.Should().Be("00000000000001");
            results.First().Should().BeEquivalentTo(expectedNationalAddress, options =>
                options.Excluding(x => x.lpi_key).Excluding(x => x.blpu_end_date).Excluding(x => x.blpu_start_date)
                    .Excluding(x => x.blpu_last_update_date).Excluding(x => x.lpi_end_date).Excluding(x => x.lpi_start_date)
                    .Excluding(x => x.lpi_last_update_date));
        }

        [Test]
        public void TransformsFirstBatchOfDataAndSaveIntoHackneyAddresses()
        {
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", "dbo.address_base");

            var addressBaseRecord = new List<CsvUploadRecord>
            {
                CreateRandomAddressBaseRecord("local"),
                CreateRandomAddressBaseRecord("local"),
                CreateRandomAddressBaseRecord("local")
            };
            addressBaseRecord.ForEach(InsertRecordIntoAddressBase);

            var handler = new Handler();
            handler.TransformData(new Handler.TransformDataRequest { limit = 2 }, new Mock<ILambdaContext>().Object);

            var results = DbConnection.Query<Address>("SELECT * FROM dbo.hackney_address").ToList();

            var expectedNationalAddresses = addressBaseRecord.Take(2).Select(MapToExpectedAddressRecord).ToList();
            results.Count.Should().Be(2);
            results.First().Should().BeEquivalentTo(expectedNationalAddresses.First(), options => options.Excluding(x => x.lpi_key));
            results.First().lpi_key.Should().Be("00000000000001");

            results.Last().Should().BeEquivalentTo(expectedNationalAddresses.Last(), options => options.Excluding(x => x.lpi_key));
            results.Last().lpi_key.Should().Be("00000000000002");
        }

        [Test]
        public void TransformsSecondBatchOfDataAndSaveIntoNationalAddresses()
        {
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", "dbo.address_base");

            var addressBaseRecord = new List<CsvUploadRecord>
            {
                CreateRandomAddressBaseRecord("local"),
                CreateRandomAddressBaseRecord("local"),
                CreateRandomAddressBaseRecord("local")
            };
            addressBaseRecord.ForEach(InsertRecordIntoAddressBase);

            var handler = new Handler();
            handler.TransformData(new Handler.TransformDataRequest { cursor = "00000000000001", limit = 2 }, new Mock<ILambdaContext>().Object);

            var results = DbConnection.Query<Address>("SELECT * FROM dbo.hackney_address").ToList();

            var expectedNationalAddresses = addressBaseRecord.Skip(1).Take(2).Select(MapToExpectedAddressRecord).ToList();
            results.Count.Should().Be(2);
            results.First().Should().BeEquivalentTo(expectedNationalAddresses.First(), options => options.Excluding(x => x.lpi_key));
            results.First().lpi_key.Should().Be("00000000000002");

            results.Last().Should().BeEquivalentTo(expectedNationalAddresses.Last(), options => options.Excluding(x => x.lpi_key));
            results.Last().lpi_key.Should().Be("00000000000003");
        }

        [Test]
        public void SavesNullDataAsNullInTheDatabase()
        {
            Environment.SetEnvironmentVariable("DB_TABLE_NAME", "dbo.address_base");

            var hackneyAddress = CreateRandomAddressBaseRecord("local");
            hackneyAddress.parent_uprn = null;
            hackneyAddress.building_name = null;
            InsertRecordIntoAddressBase(hackneyAddress);

            var handler = new Handler();
            handler.TransformData(new Handler.TransformDataRequest(), new Mock<ILambdaContext>().Object);

            var results = DbConnection.Query<Address>("SELECT * FROM dbo.hackney_address").ToList();

            var expectedAddress = MapToExpectedAddressRecord(hackneyAddress);
            results.Count.Should().Be(1);
            results.First().lpi_key.Should().Be("00000000000001");
            results.First().parent_uprn.Should().BeNull();
            results.First().pao_text.Should().BeNull();
        }

        private void InsertRecordIntoAddressBase(CsvUploadRecord addressBaseRecord)
        {
            DbConnection.Execute(
                "INSERT INTO dbo.address_base (uprn,parent_uprn,udprn,usrn,toid,classification_code,easting,northing,latitude,longitude,rpc,last_update_date,single_line_address,po_box,organisation,sub_building,building_name,building_number,street_name,locality,town_name,post_town,change_code,island,postcode,delivery_point_suffix,gss_code)" +
                "VALUES (@uprn,@parent_uprn,@udprn,@usrn,@toid,@classification_code,@easting,@northing,@latitude,@longitude,@rpc,@last_update_date,@single_line_address,@po_box,@organisation,@sub_building,@building_name,@building_number,@street_name,@locality,@town_name,@post_town,@change_code,@island,@postcode,@delivery_point_suffix,@gss_code)",
                addressBaseRecord);
        }

        private CsvUploadRecord CreateRandomAddressBaseRecord(string gazetteer)
        {
            var singleAddressLine = string.Join(',', _fixture.CreateMany<string>(4));
            var gss_code = gazetteer == "local" ? _hackneyGssCode : "E06281728";
            return _fixture.Build<CsvUploadRecord>()
                .With(c => c.gss_code, gss_code)
                .With(a => a.single_line_address, singleAddressLine)
                .Create();
        }

        private static Address MapToExpectedAddressRecord(CsvUploadRecord addressBaseRecord)
        {
            var usage = addressBaseRecord.classification_code.First() switch
            {
                'R' => "Residential",
                'C' => "commercial",
                'P' => "Parent Shell",
                'L' => "Land",
                'X' => "Dual Use",
                _ => "Unclassified"
            };
            var addressLines = addressBaseRecord.single_line_address.Split(',');

            var expectedNationalAddress = new Address
            {
                blpu_end_date = 0,
                blpu_last_update_date = 0,
                blpu_start_date = 0,
                lpi_start_date = 0,
                lpi_end_date = 0,
                lpi_last_update_date = 0,
                blpu_class = addressBaseRecord.classification_code.Substring(0, 4),
                building_number = addressBaseRecord.building_number,
                easting = addressBaseRecord.easting,
                gazetteer = addressBaseRecord.gss_code == _hackneyGssCode ? "local" : "national",
                latitude = addressBaseRecord.latitude,
                line1 = addressLines.ElementAt(0),
                line2 = addressLines.Length > 1 ? addressLines.ElementAt(1) : null,
                line3 = addressLines.Length > 2 ? addressLines.ElementAt(2) : null,
                line4 = null,
                locality = addressBaseRecord.locality,
                longitude = addressBaseRecord.longitude,
                lpi_logical_status = "Approved Preferred",
                neverexport = false,
                northing = addressBaseRecord.northing,
                organisation = addressBaseRecord.organisation,
                pao_text = addressBaseRecord.building_name,
                parent_uprn = (long?)addressBaseRecord.parent_uprn,
                planning_use_class = "",
                postcode = addressBaseRecord.postcode,
                postcode_nospace = addressBaseRecord.postcode.Replace(" ", ""),
                property_shell = addressBaseRecord.classification_code.First() == 'P',
                sao_text = addressBaseRecord.sub_building,
                street_description = addressBaseRecord.street_name,
                town = addressBaseRecord.town_name,
                uprn = (long)addressBaseRecord.uprn,
                usage_description = usage,
                usage_primary = usage,
                usrn = (int)addressBaseRecord.usrn,
                ward = "",
            };
            return expectedNationalAddress;
        }

        private static int? ConvertToIntDate(string date)
        {
            if (string.IsNullOrEmpty(date)) return null;
            var result = DateTime.ParseExact(date, "yyyy-MM-dd", null);
            return Convert.ToInt32(result.ToString("yyyyMMdd"));
        }
    }
}