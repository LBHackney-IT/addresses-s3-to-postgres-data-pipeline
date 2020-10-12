FROM mcr.microsoft.com/dotnet/core/sdk:3.1

# disable microsoft telematry
ENV DOTNET_CLI_TELEMETRY_OPTOUT='true'

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ./AddressesDataPipeline.sln ./
COPY ./AddressesDataPipeline/AddressesDataPipeline.csproj ./AddressesDataPipeline/
COPY ./AddressesDataPipeline.Tests/AddressesDataPipeline.Tests.csproj ./AddressesDataPipeline.Tests/

RUN dotnet restore ./AddressesDataPipeline/AddressesDataPipeline.csproj
RUN dotnet restore ./AddressesDataPipeline.Tests/AddressesDataPipeline.Tests.csproj

# Copy everything else and build
COPY . .

RUN dotnet build -c debug -o out AddressesDataPipeline.Tests/AddressesDataPipeline.Tests.csproj

CMD dotnet test
