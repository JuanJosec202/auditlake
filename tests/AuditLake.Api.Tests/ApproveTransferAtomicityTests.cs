using System.Security.Cryptography;
using System.Text;
using AuditLake.Api.Application;
using AuditLake.Api.Domain;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;

namespace AuditLake.Api.Tests;

public sealed class ApproveTransferAtomicityTests(SqlServerFixture sql, ITestOutputHelper output)
    : IClassFixture<SqlServerFixture>
{
    private static readonly DateTimeOffset Now = new(2026, 11, 25, 22, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Approve_persists_transfer_and_outbox_event_in_one_commit()
    {
        var cs = await sql.CreateMigratedDatabaseAsync();
        var transferId = await SeedPendingTransferAsync(cs, 1500m);

        await using (var db = SqlServerFixture.CreateContext(cs, output.WriteLine))
        {
            var result = await new ApproveTransfer(db, new FixedClock(Now)).ExecuteAsync(transferId, "ana.supervisora");
            Assert.Equal(ApproveTransferResult.Approved, result);
        }

        await using var check = SqlServerFixture.CreateContext(cs);
        var transfer = await check.Transfers.SingleAsync(t => t.Id == transferId);
        var evt = await check.Outbox.SingleAsync();

        Assert.Equal(TransferStatus.Approved, transfer.Status);
        Assert.Equal("TransferApproved", evt.EventType);
        Assert.Equal(7, evt.EventId.Version);
        Assert.Contains(transferId.ToString(), evt.Payload);
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(evt.Payload))), evt.PayloadHash);

        output.WriteLine($"Transfer {transfer.Id} -> {transfer.Status}; outbox Id={evt.Id} EventId={evt.EventId} Hash={evt.PayloadHash}");
        output.WriteLine($"Payload: {evt.Payload}");
    }

    [Fact]
    public async Task Failed_outbox_insert_rolls_back_the_approval()
    {
        var cs = await sql.CreateMigratedDatabaseAsync();
        var transferId = await SeedPendingTransferAsync(cs, 900m);

        // Falla real en SQL Server: cualquier INSERT en audit.Outbox viola esta restricción.
        await using (var admin = SqlServerFixture.CreateContext(cs))
            await admin.Database.ExecuteSqlRawAsync(
                "ALTER TABLE audit.Outbox ADD CONSTRAINT CK_Test_RejectInsert CHECK (EventType = N'__never__');");

        await using (var db = SqlServerFixture.CreateContext(cs, output.WriteLine))
        {
            var ex = await Assert.ThrowsAsync<DbUpdateException>(
                () => new ApproveTransfer(db, new FixedClock(Now)).ExecuteAsync(transferId, "ana.supervisora"));
            var sqlEx = Assert.IsType<SqlException>(ex.InnerException);
            Assert.Contains("CK_Test_RejectInsert", sqlEx.Message);
            output.WriteLine($"Error esperado de SQL Server: {sqlEx.Number} {sqlEx.Message}");
        }

        await using var check = SqlServerFixture.CreateContext(cs);
        var transfer = await check.Transfers.SingleAsync(t => t.Id == transferId);
        Assert.Equal(TransferStatus.Pending, transfer.Status);
        Assert.Null(transfer.ApprovedBy);
        Assert.Equal(0, await check.Outbox.CountAsync());

        output.WriteLine($"Después del fallo: Transfer {transfer.Id} sigue {transfer.Status}; filas en outbox = 0");
    }

    private static async Task<Guid> SeedPendingTransferAsync(string cs, decimal amount)
    {
        await using var db = SqlServerFixture.CreateContext(cs);
        var transfer = Transfer.Request(Guid.CreateVersion7(), amount);
        db.Transfers.Add(transfer);
        await db.SaveChangesAsync();
        return transfer.Id;
    }

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
