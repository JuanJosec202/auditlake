using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace AuditLake.Api.Infrastructure.Migrations
{
    /// <summary>
    /// EF Core crea la base con READ_COMMITTED_SNAPSHOT ON. La decisión del proyecto es medir antes de activar
    /// RCSI (fase 6), así que la línea base queda explícitamente en OFF.
    /// </summary>
    public partial class RcsiOffBaseline : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql(
                "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF WITH ROLLBACK IMMEDIATE;",
                suppressTransaction: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql(
                "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;",
                suppressTransaction: true);
        }
    }
}
