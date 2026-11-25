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
    }
}
