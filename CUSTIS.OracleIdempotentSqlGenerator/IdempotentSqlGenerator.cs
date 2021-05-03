using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.Logging;
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
        public IdempotentSqlGenerator(MigrationsSqlGeneratorDependencies dependencies, IOracleOptions options) 
            : base(dependencies, options)
        {
        }

        public override IReadOnlyList<MigrationCommand> Generate(IReadOnlyList<MigrationOperation> operations, IModel model = null, MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
        {
            var field = typeof(OracleMigrationsSqlGenerator)
                .GetField("_operations", BindingFlags.Instance | BindingFlags.NonPublic);
            try
            {
                if (operations == null)
                {
                    throw new ArgumentNullException(nameof(operations));
                }
                field.SetValue(this, operations);
                this.Options = options;
                var commandListBuilder = new CustomStatementBuilder(Dependencies, Dependencies.SqlGenerationHelper.StatementTerminator);
                foreach (var operation in operations)
                {
                    Generate(operation, model, commandListBuilder);
                }
                return commandListBuilder.GetCommandList();
            }
            finally
            {
                field.SetValue(this, null);
            }
        }

        #region Columns

        private string GetRowVersionTriggerName(string tableName)
        {
            return $"rowversion_{tableName}";
        }

        /// <inheritdoc />
        protected override void Generate(AddColumnOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating column {operation.Name} in table {operation.Table};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = UPPER('{operation.Table}')");
                builder.AppendLine($"AND column_name = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).EscapeQuotes = true;
                    base.Generate(operation, model, builder, false);
                    ((CustomStatementBuilder)builder).EscapeQuotes = false;
                    builder.Append("';");
                    if (operation.IsRowVersion)
                    {
                        builder.AppendLine($"EXECUTE IMMEDIATE 'CREATE OR REPLACE TRIGGER \"{GetRowVersionTriggerName(operation.Table)}\"")
                            .AppendLine($"BEFORE INSERT OR UPDATE ON \"{operation.Table}\"")
                            .AppendLine("FOR EACH ROW")
                            .AppendLine("BEGIN")
                            .AppendLine(
                                $":NEW.\"{operation.Name}\" := UTL_RAW.CAST_FROM_BINARY_INTEGER(UTL_RAW.CAST_TO_BINARY_INTEGER(NVL(:OLD.\"{operation.Name}\", ''00000000'')) + 1);")
                            .AppendLine("END;';");
                    }
                    builder.AppendLine();
                }

                builder.AppendLine("END IF;");
            });
        }

        /// <inheritdoc />
        protected override void Generate(DropColumnOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting column {operation.Name} from table {operation.Table};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = UPPER('{operation.Table}')");
                builder.AppendLine($"AND column_name = UPPER('{operation.Name}');");
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

            // dropping trigger of rowversion column
            InIdempotentWrapper(builder, terminate, () =>
            {
                var triggerName = GetRowVersionTriggerName(operation.Table);
                builder.AppendLine($"-- Drop trigger {triggerName};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM sys.all_triggers");
                builder.AppendLine($"WHERE table_name = UPPER('{operation.Table}')");
                builder.AppendLine($"AND trigger_name = '{triggerName}';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append($"EXECUTE IMMEDIATE 'DROP TRIGGER \"{triggerName}\"';");
                    builder.AppendLine();
                }

                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(RenameColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming column {operation.Table}.{operation.Name} -> {operation.NewName};");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_tab_columns");
                builder.AppendLine($"WHERE table_name = UPPER('{operation.Table}')");
                builder.AppendLine($"AND column_name = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
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
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating table {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 0 THEN");
                builder.AppendLine("BEGIN");
                using (builder.Indent())
                {
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    ((CustomStatementBuilder)builder).InsideCreateTable = true;
                    base.Generate(operation, model, builder, true);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    ((CustomStatementBuilder)builder).InsideCreateTable = false;
                }
                builder.AppendLine("");
                builder.AppendLine("END;");
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropTableOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting table {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = UPPER('{operation.Name}');");
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
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming table {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_objects");
                builder.AppendLine($"WHERE object_name = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        #endregion

        #region Index & constraint

        protected override void Generate(CreateIndexOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating index {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = UPPER('{operation.Name}');");
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
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting index {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = UPPER('{operation.Name}');");
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
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming index {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_indexes");
                builder.AppendLine($"WHERE index_name = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AddPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating primary key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'P';");
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

        protected override void Generate(DropPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate= true)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting primary key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'P';");
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


        protected override void Generate(AddForeignKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate)
        {
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Creating foreign key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'R';");
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
            InIdempotentWrapper(builder, terminate, () =>
            {
                builder.AppendLine($"-- Deleting foreign key {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'R';");
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
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Creating unique constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'U';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder) builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder) builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Deleting unique constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'U';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AddCheckConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Creating check constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'C';");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        /// <summary>
        ///     Builds commands for the given <see cref="T:Microsoft.EntityFrameworkCore.Migrations.Operations.DropCheckConstraintOperation" /> by making calls on the given
        ///     <see cref="T:Microsoft.EntityFrameworkCore.Migrations.MigrationCommandListBuilder" />, and then terminates the final command.
        /// </summary>
        /// <param name="operation"> The operation. </param>
        /// <param name="model"> The target model which may be <see langword="null" /> if the operations exist without a model. </param>
        /// <param name="builder"> The command builder to use to build the commands. </param>
        protected override void Generate(DropCheckConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Deleting unique constraint {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_constraints");
                builder.AppendLine($"WHERE constraint_name = UPPER('{operation.Name}') AND constraint_type = 'C';");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        #endregion

        #region Sequence

        protected override void Generate(CreateSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Creating sequence {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 0 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(DropSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Deleting sequence {operation.Name}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(RenameSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            InIdempotentWrapper(builder, true, () =>
            {
                builder.AppendLine($"-- Renaming sequence {operation.Name} -> {operation.NewName}");
                builder.AppendLine("SELECT COUNT(*) INTO i FROM user_sequences ");
                builder.AppendLine($"WHERE sequence_name  = UPPER('{operation.Name}');");
                builder.AppendLine("IF I = 1 THEN");
                using (builder.Indent())
                {
                    builder.Append("EXECUTE IMMEDIATE '");
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = true;
                    base.Generate(operation, model, builder);
                    ((CustomStatementBuilder)builder).IgnoreEndOfStatement = false;
                    builder.AppendLine("';");
                }
                builder.AppendLine("END IF;");
            });
        }

        protected override void Generate(AlterColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            base.Generate(operation, model, builder);
        }

        /// <summary>
        ///     <para>
        ///         Can be overridden by database providers to build commands for the given <see cref="T:Microsoft.EntityFrameworkCore.Migrations.Operations.AlterTableOperation" />
        ///         by making calls on the given <see cref="T:Microsoft.EntityFrameworkCore.Migrations.MigrationCommandListBuilder" />.
        ///     </para>
        ///     <para>
        ///         Note that the default implementation of this method does nothing because there is no common metadata
        ///         relating to this operation. Providers only need to override this method if they have some provider-specific
        ///         annotations that must be handled.
        ///     </para>
        /// </summary>
        /// <param name="operation"> The operation. </param>
        /// <param name="model"> The target model which may be <see langword="null" /> if the operations exist without a model. </param>
        /// <param name="builder"> The command builder to use to build the commands. </param>
        protected override void Generate(AlterTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            base.Generate(operation, model, builder);
        }

        protected override void Generate(AlterSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            //You can perform ALTER as many times ad you need
            base.Generate(operation, model, builder);
        }

        protected override void Generate(RestartSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            // At time of 2020-02-17 OracleMigrationsSqlGenerator has no support of RestartSequenceOperation
            // It generates the code, that couldn't be run on OracleDB (ALTER SEQUENCE...RESTART WITH)
            base.Generate(operation, model, builder);
        }

        #endregion

        #region Help code

        /// <summary> Generate code inside idempotent wrapper </summary>
        protected virtual void InIdempotentWrapper(MigrationCommandListBuilder builder, bool terminate, 
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