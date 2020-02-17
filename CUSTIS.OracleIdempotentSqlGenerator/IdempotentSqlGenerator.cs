using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Oracle.EntityFrameworkCore.Infrastructure.Internal;
using Oracle.EntityFrameworkCore.Migrations;

namespace CUSTIS.OracleIdempotentSqlGenerator
{
    /// <summary>
    /// Idempotent SQL generator for Oracle
    /// </summary>
    public class IdempotentSqlGenerator : OracleMigrationsSqlGenerator
    {
        /// <summary>
        /// Idempotent SQL generator for Oracle
        /// </summary>
        public IdempotentSqlGenerator(MigrationsSqlGeneratorDependencies dependencies, IOracleOptions options, IDiagnosticsLogger<DbLoggerCategory.Migrations> logger = null) 
            : base(dependencies, options, logger)
        {
        }

        public override IReadOnlyList<MigrationCommand> Generate(IReadOnlyList<MigrationOperation> operations, IModel model = null)
        {
            var builder = new IgnoreEndOfStatementBuilder(Dependencies.CommandBuilderFactory, Dependencies.SqlGenerationHelper.StatementTerminator);
            foreach (var operation in operations)
            {
                Generate(operation, model, builder);
            }

            return builder.GetCommandList();
        }

        #region Columns

        /// <inheritdoc />
        protected override void Generate(AddColumnOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating column {operation.Name} in table {operation.Table};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = '{operation.Table}'");
                builder.AppendLine($"AND column_name = '{operation.Name}';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.Append("';");
                    builder.AppendLine();
                }

                builder.AppendLine("END IF;");
            });
        }

        /// <inheritdoc />
        protected override void Generate(DropColumnOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting column {operation.Name} from table {operation.Table};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = '{operation.Table}'");
                builder.AppendLine($"AND column_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.Append("';");
                    builder.AppendLine();
                }

                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(RenameColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming column {operation.Table}.{operation.Name} -> {operation.NewName};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = '{operation.Table}'");
                builder.AppendLine($"AND column_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.Append("';");
                    builder.AppendLine();
                }

                builder.AppendLine("END IF;");
            });
        }

        #endregion

        #region Table

        /// <inheritdoc />
        protected override void Generate(CreateTableOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating table {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = '{operation.Name}';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder, true);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                }

                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropTableOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting table {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(RenameTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming table {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        #endregion

        #region Index & constraint

        protected override void Generate(CreateIndexOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating index {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = '{operation.Name}';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropIndexOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting index {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }


        protected override void Generate(RenameIndexOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming index {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AddPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating primary key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'P';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting primary key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'P';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(operation, model, builder, true);
        }

        protected override void Generate(AddForeignKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating foreign key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'R';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropForeignKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            Generate(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting foreign key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'R';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    base.Generate(operation, model, builder, false);
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AddUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Creating unique constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'U';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder) builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder) builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Deleting unique constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = '{operation.Name}' AND constraint_type = 'U';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        #endregion

        #region Sequence

        protected override void Generate(CreateSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Creating sequence {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = '{operation.Name}';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Deleting sequence {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(RenameSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Generate(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming sequence {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = '{operation.Name}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((IgnoreEndOfStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AlterSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            //You can perform ALTER as many times ad you need
            base.Generate(operation, model, builder);
        }

        protected override void Generate(RestartSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotImplementedException("Oracle doesn't support restart of sequence. See https://stackoverflow.com/a/93633 for details");
        }

        #endregion

        #region Help code

        private void Generate(MigrationCommandListBuilder builder, bool terminate, 
            Action generate)
        {
            builder.AppendLine("DECLARE");
            using (builder.Indent())
            {
                builder.AppendLine("i NUMBER;");
            }

            builder.AppendLine("BEGIN");

            using (builder.Indent())
            {
                generate(); 
            }

            builder.AppendLine("END;");

            if (terminate)
            {
                EndStatement(builder);
            }
        }

        #endregion
    }
}