# S3 to Postgres Data Pipeline Template

A lambda function that is called when a CSV file is added to an S3 bucket.

This is a template repository. Please create a new repository for your pipeline by using this as your base template. 

*Ensure you have renamed files and functions accordinly based on your data pipeline.*

## Functionality included in this template

- Lambda is configured to be triggered upon upload object event on a given S3 bucket
- Lambda role set up is added to serverless.yml to ensure read access to RDS
- Truncate table in an existing database
- Copy data from CSV uploaded to an S3 bucket to a Postgres table
- CircleCI deployment pipeline configuration

## Dependencies

- Dotnet Core 3.1
- Node.js 13.11

## Working on this project

```bash
  dotnet restore
  make build
```

## Running the tests

```bash
  make test
```

## Set up required

You will need to rename all files, properties and functions accordingly. 

The following environment variables will need to be set within your Lambda function once deployed

- DB_TABLE_NAME
  - Table name to be truncated and where the data is stored
- DB_HOST
  - Database host e.g. Postgres endpoint URL
- DB_PORT
  - Set this up only if Postgres is not running on default port
- DB_USERNAME
  - Database login username 
- DB_PASSWORD
  - Database login password
- DB_DATABASE
  - Database name

*You will also need to create an AWS S3 bucket and specify it within the serverless.yml file.*

## Deploying the application

Deployment jobs are included in the CircleCI workflow configuration.
