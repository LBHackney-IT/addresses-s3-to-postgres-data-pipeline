FROM mcr.microsoft.com/dotnet/core/sdk:3.1

# disable microsoft telematry
ENV DOTNET_CLI_TELEMETRY_OPTOUT='true'

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ./S3ToPostgresDataPipeline.sln ./
COPY ./S3ToPostgresDataPipeline/S3ToPostgresDataPipeline.csproj ./S3ToPostgresDataPipeline/
COPY ./S3ToPostgresDataPipeline.Tests/S3ToPostgresDataPipeline.Tests.csproj ./S3ToPostgresDataPipeline.Tests/

RUN dotnet restore ./S3ToPostgresDataPipeline/S3ToPostgresDataPipeline.csproj
RUN dotnet restore ./S3ToPostgresDataPipeline.Tests/S3ToPostgresDataPipeline.Tests.csproj

# Copy everything else and build
COPY . .

RUN dotnet build -c debug -o out S3ToPostgresDataPipeline.Tests/S3ToPostgresDataPipeline.Tests.csproj

CMD dotnet test
