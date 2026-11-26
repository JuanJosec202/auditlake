using AuditLake.Api.Application;
using AuditLake.Api.Domain;
using AuditLake.Api.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Hiper-logs (best effort): Serilog JSON (CLEF) a archivos rotados; nunca pasan por SQL Server.
// La rotación por minuto es configuración de demo (appsettings.json).
builder.Services.AddSerilog((services, lc) => lc.ReadFrom.Configuration(builder.Configuration));

builder.Services.AddDbContext<AuditLakeDbContext>(o =>
    o.UseSqlServer(builder.Configuration.GetConnectionString("AuditLake")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.Configure<AuditOptions>(builder.Configuration.GetSection(AuditOptions.Section));
builder.Services.AddScoped<ApproveTransfer>();

var app = builder.Build();

app.UseSerilogRequestLogging();

if (app.Configuration.GetValue<bool>("ApplyMigrationsOnStartup"))
{
    using var scope = app.Services.CreateScope();
    await scope.ServiceProvider.GetRequiredService<AuditLakeDbContext>().Database.MigrateAsync();
}

app.MapGet("/health", () => Results.Ok("ok"));

app.MapPost("/transfers", async (CreateTransferRequest req, AuditLakeDbContext db, CancellationToken ct) =>
{
    var transfer = Transfer.Request(Guid.CreateVersion7(), req.Amount);
    db.Transfers.Add(transfer);
    await db.SaveChangesAsync(ct);
    return Results.Created($"/transfers/{transfer.Id}", new { transfer.Id, transfer.Amount, Status = transfer.Status.ToString() });
});

app.MapPost("/transfers/{id:guid}/approve", async (Guid id, ApproveTransferRequest req, ApproveTransfer useCase, CancellationToken ct) =>
    await useCase.ExecuteAsync(id, req.ApprovedBy, ct) switch
    {
        ApproveTransferResult.Approved => Results.NoContent(),
        ApproveTransferResult.NotFound => Results.NotFound(),
        _ => Results.Conflict(),
    });

app.Run();

public sealed record CreateTransferRequest(decimal Amount);
public sealed record ApproveTransferRequest(string ApprovedBy);
