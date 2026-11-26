using AuditLake.Api.Domain;
using Microsoft.EntityFrameworkCore;

namespace AuditLake.Api.Infrastructure;

public sealed class AuditLakeDbContext(DbContextOptions<AuditLakeDbContext> options) : DbContext(options)
{
    public DbSet<Transfer> Transfers => Set<Transfer>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Transfer>(e =>
        {
            e.ToTable("Transfers", "dbo");
            e.HasKey(t => t.Id);
            e.Property(t => t.Id).ValueGeneratedNever();
            e.Property(t => t.Amount).HasPrecision(18, 2);
            e.Property(t => t.Status).HasConversion<string>().HasMaxLength(20);
            e.Property(t => t.ApprovedBy).HasMaxLength(100);
        });

        modelBuilder.Entity<OutboxMessage>(e =>
        {
            e.ToTable("Outbox", "audit");
            e.HasKey(o => o.Id).IsClustered();
            e.Property(o => o.Id).UseIdentityColumn();
            e.Property(o => o.EventType).HasMaxLength(200);
            e.Property(o => o.Payload).HasColumnType("nvarchar(max)");
            e.Property(o => o.PayloadHash).HasColumnType("char(64)");
        });

        modelBuilder.Entity<OutboxQuarantineEntry>(e =>
        {
            e.ToTable("OutboxQuarantine", "audit");
            e.HasKey(q => q.Id).IsClustered();
            e.Property(q => q.Id).UseIdentityColumn();
            e.Property(q => q.EventType).HasMaxLength(200);
            e.Property(q => q.Payload).HasColumnType("nvarchar(max)");
            e.Property(q => q.PayloadHash).HasColumnType("char(64)");
            e.Property(q => q.ArchivedHash).HasColumnType("char(64)");
            e.Property(q => q.Reason).HasMaxLength(200);
            e.Property(q => q.RunId).HasMaxLength(64);
            e.Property(q => q.QuarantinedAt).HasDefaultValueSql("SYSDATETIMEOFFSET()");
            e.HasIndex(q => q.EventId);
        });

        modelBuilder.Entity<LegacyAuditLogEntry>(e =>
        {
            e.ToTable("AuditLog", "legacy");
            e.HasKey(l => l.Id).IsClustered();
            e.Property(l => l.Id).UseIdentityColumn();
            e.Property(l => l.EventType).HasMaxLength(200);
            e.Property(l => l.Amount).HasPrecision(18, 2);
            e.Property(l => l.ApprovedBy).HasMaxLength(100);
            e.HasIndex(l => l.OccurredAt); // backfill, conciliación y purga por rango de fechas
        });
    }

    public DbSet<LegacyAuditLogEntry> LegacyAuditLog => Set<LegacyAuditLogEntry>();

    public DbSet<OutboxQuarantineEntry> OutboxQuarantine => Set<OutboxQuarantineEntry>();
}
