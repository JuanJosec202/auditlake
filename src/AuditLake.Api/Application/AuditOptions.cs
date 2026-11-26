namespace AuditLake.Api.Application;

public enum AuditSink
{
    /// <summary>Diseño de la PoC: el evento va a audit.Outbox y el ingestor lo archiva en el Lakehouse.</summary>
    Outbox,

    /// <summary>Solo para el benchmark complementario: patrón anterior, escribir en la tabla de auditoría de SQL.</summary>
    LegacyTable,
}

public sealed class AuditOptions
{
    public const string Section = "Audit";
    public AuditSink Sink { get; set; } = AuditSink.Outbox;
}
