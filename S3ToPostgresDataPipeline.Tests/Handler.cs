using Amazon.Lambda.Core;
using Amazon.S3.Model;
using Amazon.S3.Util;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication.Cookies;
using Moq;
using Npgsql;
using NUnit.Framework;
using S3ToPostgresDataPipeline.Database;
using System;
using System.Collections.Generic;
using System.Linq;

namespace S3ToPostgresDataPipeline.Tests
{
    [TestFixture]
    public class HandlerTest : DatabaseTests
    {
        [Test]
        public void CanLoadACsvIntoTheDatabase()
        {
            var mockDatabaseActions = new Mock<IDatabaseActions>();
            var handler = new Handler(mockDatabaseActions.Object);

            var bucketData = new S3EventNotification.S3Entity() { Bucket = new S3EventNotification.S3BucketEntity() { Name = "testBucket"},
                Object = new S3EventNotification.S3ObjectEntity { Key = "test/key.csv" } };
            //S3 record mock
            var testRecord = new S3EventNotification.S3EventNotificationRecord();
            testRecord.AwsRegion = "eu-west-2";
            testRecord.S3 = bucketData;

            var s3EventMock = new S3EventNotification();
            s3EventMock.Records = new List<S3EventNotification.S3EventNotificationRecord> { testRecord };

            var contextMock = new Mock<ILambdaContext>();
            //set up Database actions
            mockDatabaseActions.Setup(x => x.CopyDataToDatabase(contextMock.Object, testRecord.AwsRegion, bucketData.Bucket.Name, bucketData.Object.Key));
            mockDatabaseActions.Setup(x => x.TruncateTable(contextMock.Object, It.IsAny<string>()));
            mockDatabaseActions.Setup(x => x.SetupDatabase(contextMock.Object)).Returns(()=>new NpgsqlConnection());

            Assert.DoesNotThrow(() => handler.LoadCsv(s3EventMock, contextMock.Object));
            mockDatabaseActions.Verify(y => y.SetupDatabase(contextMock.Object), Times.Once);
            mockDatabaseActions.Verify(y => y.TruncateTable(contextMock.Object, It.IsAny<string>()), Times.Once);
            mockDatabaseActions.Verify(y => y.CopyDataToDatabase(contextMock.Object, testRecord.AwsRegion, bucketData.Bucket.Name, bucketData.Object.Key), Times.Once);
        }
    }
}

// Example S3 notification event
// {
//   "Records": [
//     {
//       "eventVersion": "2.1",
//       "eventSource": "aws:s3",
//       "awsRegion": "us-east-2",
//       "eventTime": "2019-09-03T19:37:27.192Z",
//       "eventName": "ObjectCreated:Put",
//       "userIdentity": {
//         "principalId": "AWS:AIDAINPONIXQXHT3IKHL2"
//       },
//       // ...
//       "s3": {
//         "s3SchemaVersion": "1.0",
//         "configurationId": "828aa6fc-f7b5-4305-8584-487c791949c1",
//         "bucket": {
//           "name": "lambda-artifacts-deafc19498e3f2df",
//           "ownerIdentity": {
//             "principalId": "A3I5XTEXAMAI3E"
//           },
//           "arn": "arn:aws:s3:::lambda-artifacts-deafc19498e3f2df"
//         },
//         "object": {
//           "key": "b21b84d653bb07b05b1e6b33684dc11b",
//           "size": 1305107,
//           "eTag": "b21b84d653bb07b05b1e6b33684dc11b",
//           "sequencer": "0C0F6F405D6ED209E1"
//         }
//       }
//     }
//   ]
// }
