using AuditLake.Api.Infrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;

namespace AuditLake.Api.Tests;

/// <summary>Un contenedor SQL Server real por clase de tests; cada test usa su propia base migrada.</summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    public const string Image = "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04";

    private readonly MsSqlContainer _container = new MsSqlBuilder(Image).Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public async Task<string> CreateMigratedDatabaseAsync()
    {
        var cs = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            InitialCatalog = $"auditlake_{Guid.NewGuid():N}",
        }.ConnectionString;

        await using var db = CreateContext(cs);
        await db.Database.MigrateAsync();
        return cs;
    }

    public static AuditLakeDbContext CreateContext(string connectionString, Action<string>? log = null)
    {
        var options = new DbContextOptionsBuilder<AuditLakeDbContext>().UseSqlServer(connectionString);
        if (log is not null)
            options.LogTo(log, [DbLoggerCategory.Database.Command.Name, DbLoggerCategory.Database.Transaction.Name],
                Microsoft.Extensions.Logging.LogLevel.Debug);
        return new AuditLakeDbContext(options.Options);
    }
}
