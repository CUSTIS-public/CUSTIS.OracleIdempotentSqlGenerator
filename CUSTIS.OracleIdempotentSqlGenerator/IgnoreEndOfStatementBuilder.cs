using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CUSTIS.OracleIdempotentSqlGenerator
{
    /// <summary>
    /// <see cref="MigrationCommandListBuilder"/>, which can ignore EndOfStatement
    /// </summary>
    internal class IgnoreEndOfStatementBuilder : MigrationCommandListBuilder
    {
        private readonly string _statementTerminator;

        public IgnoreEndOfStatementBuilder(MigrationsSqlGeneratorDependencies deps, string statementTerminator) : base(deps)
        {
            _statementTerminator = statementTerminator;
        }

        public bool IgnoreEndOfStatement { get; set; } = false;


        public override MigrationCommandListBuilder AppendLine(string o)
        {
            if (IgnoreEndOfStatement && o == _statementTerminator)
            {
                return this;
            }
            return base.AppendLine(o);
        }

        public override MigrationCommandListBuilder EndCommand(bool suppressTransaction = false)
        {
            if (IgnoreEndOfStatement)
            {
                return this;
            }
            return base.EndCommand(suppressTransaction);
        }
    }
}