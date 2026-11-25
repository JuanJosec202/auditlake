using AuditLake.Api.Infrastructure;

namespace AuditLake.Api.Application;

public enum ApproveTransferResult
{
    Approved,
    NotFound,
    NotPending,
}

/// <summary>Aprueba una transferencia y registra TransferApproved en audit.Outbox en la misma transacción.</summary>
public sealed class ApproveTransfer(AuditLakeDbContext db, TimeProvider clock)
{
    public async Task<ApproveTransferResult> ExecuteAsync(Guid transferId, string approvedBy, CancellationToken ct = default)
    {
        var transfer = await db.Transfers.FindAsync([transferId], ct);
        if (transfer is null) return ApproveTransferResult.NotFound;

        Domain.TransferApproved approved;
        try
        {
            approved = transfer.Approve(approvedBy, clock.GetUtcNow());
        }
        catch (InvalidOperationException)
        {
            return ApproveTransferResult.NotPending;
        }

        db.Outbox.Add(OutboxMessage.Create(approved, approved.ApprovedAt));

        // Un solo SaveChangesAsync: UPDATE de la transferencia + INSERT en la outbox, o ninguno.
        await db.SaveChangesAsync(ct);
        return ApproveTransferResult.Approved;
    }
}
