using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Configuration;

namespace CUSTIS.OracleIdempotentSqlGenerator.Tests
{
    public class IdempotentDbContext : DbContext
    {
        private const string ConnectionStringTemplate = "User Id={0};Password={1};Data Source={2}";
        private readonly string ConnectionString;

        public IdempotentDbContext()
        {
            var builder = new ConfigurationBuilder()
                .AddUserSecrets<IdempotentDbContext>();

            var cfg = builder.Build();
            ConnectionString = string.Format(ConnectionStringTemplate, cfg["Schema"], cfg["Pwd"], cfg["DB"]);
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseOracle(ConnectionString);
            optionsBuilder.ReplaceService<IMigrationsSqlGenerator, IdempotentSqlGenerator>();
        }
    }
}