namespace AuditLake.Api.Domain;

public enum TransferStatus
{
    Pending,
    Approved,
}

public sealed class Transfer
{
    private Transfer() { }

    public Guid Id { get; private set; }
    public decimal Amount { get; private set; }
    public TransferStatus Status { get; private set; }
    public string? ApprovedBy { get; private set; }
    public DateTimeOffset? ApprovedAt { get; private set; }

    public static Transfer Request(Guid id, decimal amount)
    {
        if (amount <= 0) throw new ArgumentOutOfRangeException(nameof(amount), "El monto debe ser positivo.");
        return new Transfer { Id = id, Amount = amount, Status = TransferStatus.Pending };
    }

    public TransferApproved Approve(string approvedBy, DateTimeOffset now)
    {
        if (Status != TransferStatus.Pending)
            throw new InvalidOperationException($"La transferencia {Id} no está pendiente.");

        Status = TransferStatus.Approved;
        ApprovedBy = approvedBy;
        ApprovedAt = now;
        return new TransferApproved(Id, Amount, approvedBy, now);
    }
}

public sealed record TransferApproved(Guid TransferId, decimal Amount, string ApprovedBy, DateTimeOffset ApprovedAt);
