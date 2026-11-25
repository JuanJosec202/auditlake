using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AuditLake.Api.Infrastructure;

/// <summary>Fila de audit.Outbox. Su presencia significa "pendiente de archivar".</summary>
public sealed class OutboxMessage
{
    private static readonly JsonSerializerOptions CanonicalJson = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
    };

    private OutboxMessage() { }

    public long Id { get; private set; }
    public Guid EventId { get; private set; }
    public string EventType { get; private set; } = default!;
    public int SchemaVersion { get; private set; }
    public DateTimeOffset OccurredAt { get; private set; }
    public string Payload { get; private set; } = default!;
    public string PayloadHash { get; private set; } = default!;

    public static OutboxMessage Create<TEvent>(TEvent @event, DateTimeOffset occurredAt, int schemaVersion = 1)
        where TEvent : notnull
    {
        // Se guarda exactamente el texto canónico, así el hash se puede recalcular con sha2(payload, 256).
        var payload = JsonSerializer.Serialize(@event, CanonicalJson);
        return new OutboxMessage
        {
            EventId = Guid.CreateVersion7(occurredAt),
            EventType = typeof(TEvent).Name,
            SchemaVersion = schemaVersion,
            OccurredAt = occurredAt,
            Payload = payload,
            PayloadHash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(payload))),
        };
    }
}
