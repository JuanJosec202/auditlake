-- Siembra legacy.AuditLog con varios meses de auditoría histórica (solo si está vacía).
-- 2026-05-01 .. 2026-10-31: 184 días x 200 filas = 36.800 filas. Ejecutar:
--   docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d AuditLake -i /dev/stdin < sql/seed_legacy_auditlog.sql
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM legacy.AuditLog)
BEGIN
    DECLARE @from date = '2026-05-01', @days int = 184, @perDay int = 200;
    WITH d AS (SELECT TOP (@days) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n FROM sys.all_objects),
         r AS (SELECT TOP (@perDay) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n FROM sys.all_objects)
    INSERT INTO legacy.AuditLog (OccurredAt, EventType, TransferId, Amount, ApprovedBy)
    SELECT TODATETIMEOFFSET(DATEADD(second, r.n * 432 + 7, CAST(DATEADD(day, d.n, @from) AS datetime2(7))), '+00:00'),
           N'TransferApproved',
           NEWID(),
           CAST(10 + (ABS(CHECKSUM(NEWID())) % 500000) / 100.0 AS decimal(18, 2)),
           CONCAT(N'legacy.user', ABS(CHECKSUM(NEWID())) % 25)
    FROM d CROSS JOIN r
    ORDER BY d.n, r.n;
END
SELECT COUNT_BIG(*) AS filas, MIN(OccurredAt) AS desde, MAX(OccurredAt) AS hasta FROM legacy.AuditLog;
