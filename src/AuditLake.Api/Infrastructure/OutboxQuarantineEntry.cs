namespace AuditLake.Api.Infrastructure;

/// <summary>
/// Fila de audit.OutboxQuarantine: copia completa de la fila entrante de audit.Outbox que llegó con un
/// event_id ya archivado y otro hash. La escribe solo el ingestor (DELETE ... OUTPUT ... INTO); la API
/// únicamente define el esquema.
/// </summary>
public sealed class OutboxQuarantineEntry
{
    private OutboxQuarantineEntry() { }

    public long Id { get; private set; }
    public long OutboxId { get; private set; }
    public Guid EventId { get; private set; }
    public string EventType { get; private set; } = default!;
    public int SchemaVersion { get; private set; }
    public DateTimeOffset OccurredAt { get; private set; }
    public string Payload { get; private set; } = default!;
    public string PayloadHash { get; private set; } = default!;
    public string ArchivedHash { get; private set; } = default!;
    public string Reason { get; private set; } = default!;
    public string RunId { get; private set; } = default!;
    public DateTimeOffset QuarantinedAt { get; private set; }
}
