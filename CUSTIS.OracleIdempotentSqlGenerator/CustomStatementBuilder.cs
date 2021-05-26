using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CUSTIS.OracleIdempotentSqlGenerator
{
    /// <summary>
    /// <see cref="MigrationCommandListBuilder"/>, which can ignore EndOfStatement and rewrite some statements
    /// </summary>
    internal class CustomStatementBuilder : MigrationCommandListBuilder
    {
        private readonly string _statementTerminator;

        public CustomStatementBuilder(MigrationsSqlGeneratorDependencies deps, string statementTerminator) : base(deps)
        {
            _statementTerminator = statementTerminator;
        }

        public bool IgnoreEndOfStatement { get; set; } = false;

        public bool InsideCreateTable { get; set; } = false;

        private bool InsideCreateRowVersionTrigger { get; set; } = false;

        private bool InsideCreateComment { get; set; } = false;

        public bool InsideAddColumn { get; set; } = false;

        public bool EscapeQuotes { get; set; } = false;

        /// <summary>
        ///     Appends the given string to the command being built.
        /// </summary>
        /// <param name="o"> The string to append. </param>
        /// <returns> This builder so that additional calls can be chained. </returns>
        public override MigrationCommandListBuilder Append(string o)
        {
            if (EscapeQuotes)
            {
                o = o.Replace("'", "''");
            }

            if (InsideCreateTable && o == "CREATE OR REPLACE TRIGGER ")
            {
                InsideCreateRowVersionTrigger = true;
                EscapeQuotes = true;
                // Wrap creation of row version trigger into EXECUTE IMMEDIATE
                return base.Append($"EXECUTE IMMEDIATE '{o}");
            }

            if (InsideCreateRowVersionTrigger && o == "END")
            {
                InsideCreateRowVersionTrigger = false;
                EscapeQuotes = false;
                // Wrap creation of row version trigger into EXECUTE IMMEDIATE -- end
                return base.AppendLine("; END;';");
            }

            if ((InsideCreateTable || InsideAddColumn) && (o == "COMMENT ON TABLE " || o == "COMMENT ON COLUMN "))
            {
                InsideCreateComment = true;
                EscapeQuotes = true;
                // Wrap creation of comment into EXECUTE IMMEDIATE
                if (InsideAddColumn) base.AppendLine("';");
                return base.Append($"EXECUTE IMMEDIATE '{o}");
            }

            if (InsideCreateComment && o.StartsWith("N'"))
            {
                // In case of data we have meta-information from model about actual storage types for strings
                // (varchar2 or nvarchar2, ansi or unicode). But in case of comments there is no meta-information.
                // Provider regards them as unicode by default and prefixes with "N". Actual storage type
                // for comments is ansi, so we have to cut off the prefix.
                return base.Append(o.Substring(1));
            }

            return base.Append(o);   
        }

        public override MigrationCommandListBuilder AppendLine(string o)
        {
            if (o != _statementTerminator)
            {
                return base.AppendLine(o);
            }

            if (InsideCreateComment)
            {
                InsideCreateComment = false;
                EscapeQuotes = false;
                // Wrap creation of comment into EXECUTE IMMEDIATE -- end
                if (InsideCreateTable) base.AppendLine("';");
                return this;
            }

            if (IgnoreEndOfStatement)
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