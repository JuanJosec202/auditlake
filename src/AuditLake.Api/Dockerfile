FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/AuditLake.Api/AuditLake.Api.csproj src/AuditLake.Api/
RUN dotnet restore src/AuditLake.Api/AuditLake.Api.csproj
COPY src/AuditLake.Api/ src/AuditLake.Api/
RUN dotnet publish src/AuditLake.Api/AuditLake.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
# Directorios de hiper-logs compartidos con log_ingest (otro uid): el volumen applogs hereda estos permisos.
RUN mkdir -p /app/logs/active /app/logs/ready /app/logs/archive && chmod -R 0777 /app/logs
USER app
EXPOSE 8080
ENTRYPOINT ["dotnet", "AuditLake.Api.dll"]
