namespace AuditLake.Api.Infrastructure;

/// <summary>
/// Fila de legacy.AuditLog: la tabla histórica de auditoría que se descarga una sola vez al Lakehouse
/// (main --backfill) y luego se purga en lotes. La API ya no escribe aquí; solo define el esquema.
/// </summary>
public sealed class LegacyAuditLogEntry
{
    private LegacyAuditLogEntry() { }

    public long Id { get; private set; }
    public DateTimeOffset OccurredAt { get; private set; }
    public string EventType { get; private set; } = default!;
    public Guid TransferId { get; private set; }
    public decimal Amount { get; private set; }
    public string ApprovedBy { get; private set; } = default!;

    public static LegacyAuditLogEntry From(Domain.TransferApproved e) => new()
    {
        OccurredAt = e.ApprovedAt,
        EventType = nameof(Domain.TransferApproved),
        TransferId = e.TransferId,
        Amount = e.Amount,
        ApprovedBy = e.ApprovedBy,
    };
}
