﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;
using Xunit.Abstractions;

namespace CUSTIS.OracleIdempotentSqlGenerator.Tests
{
    /// <summary>
    /// Tests on <see cref="IdempotentSqlGenerator"/>
    /// </summary>
    public class IdempotentSqlGeneratorTests
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly IMigrationsSqlGenerator _sqlGenerator;

        private readonly IdempotentDbContext _dbContext = new();
        private DatabaseFacade Database => _dbContext.Database;


        private const string TableName = "TEST_TABLE";
        private const string NewTableName = "TEST_TABLE_NEW";
        private const string Column1Name = "COLUMN_1";
        private const string Column2Name = "COLUMN_2";
        private const string Index1Name = "IX_COLUMN_1";
        private const string Index1NewName = "IX_COLUMN_1_NEW";
        private const string Fk1Name = "FK_COLUMN_1";
        private const string SequenceName = "TEST_SEQ_1";
        private const string NewSequenceName = "NEW_TEST_SEQ_1";
        private const string RowVersionColumnName = "TIMESTAMP";
        private const string TableComment = "Table comment";
        private const string ColumnComment = "Column comment";

        public IdempotentSqlGeneratorTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            _sqlGenerator = _dbContext.GetService<IMigrationsSqlGenerator>();
        }

        #region Table

        [Fact]
        public void Generate_CreateTableOperation_IsIdempotent()
        {
            //Arrange
            DropTestTable();
            var operations = new[]
            {
                new CreateTableOperation
                {
                    Name = TableName,
                    Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = Column1Name,
                            Table = TableName,
                            ClrType = typeof(int),
                            ColumnType = "number"
                        }
                    }
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesTableExist(TableName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesTableExist(TableName));
        }

        [Fact]
        public void Generate_CreateTableWithRowVersion_CreatesTableWithRowVersion()
        {
            //Arrange
            DropTestTable();
            var operations = new[]
            {
                new CreateTableOperation
                {
                    Name = TableName,
                    Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = Column1Name,
                            Table = TableName,
                            ClrType = typeof(int),
                            ColumnType = "number"
                        },
                        new AddColumnOperation
                        {
                            Name = RowVersionColumnName,
                            Table = TableName,
                            ClrType = typeof(byte[]),
                            ColumnType = "RAW(8)",
                            IsRowVersion = true,
                            IsNullable = false,
                            DefaultValue = new byte[0]
                        }
                    }
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesTableExist(TableName));
            ExecuteCommands(commands);
            Assert.True(DoesTableExist(TableName));

            Database.ExecuteSqlRaw($"INSERT INTO {TableName} ({Column1Name}) VALUES (1)");
            var initialTimestamp = ToInt64(ExecuteScalar<byte[]>($"SELECT {RowVersionColumnName} FROM {TableName}"));
            Database.ExecuteSqlRaw($"UPDATE {TableName} SET {Column1Name} = 2");
            var newTimestamp = ToInt64(ExecuteScalar<byte[]>($"SELECT {RowVersionColumnName} FROM {TableName}"));
            Assert.NotEqual(initialTimestamp, newTimestamp);
        }

        private long ToInt64(byte[] byteArray)
        {
            return BitConverter.ToInt64(new byte[] {0, 0, 0, 0}.Concat(byteArray).ToArray());
        }

        [Fact]
        public void Generate_DropTableOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[] {new DropTableOperation {Name = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesTableExist(TableName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesTableExist(TableName));
        }

        [Fact]
        public void Generate_RenameTableOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new RenameTableOperation
                {
                    Name = TableName,
                    NewName = NewTableName
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesTableExist(TableName));
            Assert.False(DoesTableExist(NewTableName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesTableExist(TableName));
            Assert.True(DoesTableExist(NewTableName));
        }

        private bool DoesTableExist(string tableName)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM user_objects " +
                                             $"WHERE object_name = '{tableName}'");
            return res != 0;
        }

        private void ReCreateTestTable()
        {
            DropTestTable();
            CreateTestTable(new AddColumnOperation
            {
                Name = Column1Name,
                Table = TableName,
                ClrType = typeof(int),
                ColumnType = "number"
            });
        }

        private void CreateTestTable(params AddColumnOperation[] columns)
        {
            var createTableOperation = new CreateTableOperation
            {
                Name = TableName
            };
            createTableOperation.Columns.AddRange(columns);
            var operations = new[]
            {
                createTableOperation
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void DropTestTable()
        {
            var operations = new[]
            {
                new DropTableOperation {Name = TableName},
                new DropTableOperation {Name = NewTableName}
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        #endregion

        #region Indexes and Constraints

        [Fact]
        public void Generate_AddPrimaryKeyOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AddPrimaryKeyOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    Columns = new[] {Column1Name}
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesPrimaryKeyExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesPrimaryKeyExist(Column1Name));
        }

        [Fact]
        public void Generate_DropPrimaryKeyOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreatePrimaryKey(Column1Name);
            var operations = new[] {new DropPrimaryKeyOperation {Name = Column1Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesPrimaryKeyExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesPrimaryKeyExist(Column1Name));
        }

        [Fact]
        public void Generate_AddForeignKeyOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreatePrimaryKey(Column1Name);
            var operations = new[]
            {
                new AddForeignKeyOperation
                {
                    Name = Fk1Name,

                    Table = TableName,
                    Columns = new[] {Column1Name},
                    PrincipalTable = TableName,
                    PrincipalColumns = new[] {Column1Name}
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesForeignKeyExist(Fk1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesForeignKeyExist(Fk1Name));
        }

        [Fact]
        public void Generate_DropForeignKeyOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateForeignKey(Fk1Name);
            var operations = new[] {new DropForeignKeyOperation {Name = Fk1Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesForeignKeyExist(Fk1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesForeignKeyExist(Fk1Name));
        }

        [Fact]
        public void Generate_AddUniqueConstraintOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AddUniqueConstraintOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    Columns = new[] {Column1Name}
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesUniqueConstraintExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesUniqueConstraintExist(Column1Name));
        }

        [Fact]
        public void Generate_DropUniqueConstraintOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateUniqueConstraint(Column1Name);
            var operations = new[] {new DropUniqueConstraintOperation {Name = Column1Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesUniqueConstraintExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesUniqueConstraintExist(Column1Name));
        }

        [Fact]
        public void Generate_AddCheckConstraintOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AddCheckConstraintOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    Sql = $"{Column1Name} BETWEEN 1 AND 2"
                }
            };

            //Act 
            var commands1 = _sqlGenerator.Generate(operations);
            var commands = commands1;

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesCheckConstraintExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesCheckConstraintExist(Column1Name));
        }

        [Fact]
        public void Generate_DropCheckConstraintOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateCheckConstraint(Column1Name);
            var operations = new[] {new DropCheckConstraintOperation {Name = Column1Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesCheckConstraintExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesCheckConstraintExist(Column1Name));
        }

        [Fact]
        public void Generate_CreateIndexOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new CreateIndexOperation()
                {
                    Name = Index1Name,
                    Table = TableName,
                    Columns = new[] {Column1Name}
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesIndexExist(Index1Name));
        }


        [Fact]
        public void Generate_DropIndexOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateIndex(Index1Name, Column1Name);
            var operations = new[] {new DropIndexOperation {Name = Index1Name}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesIndexExist(Index1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesIndexExist(Index1Name));
        }

        [Fact]
        public void Generate_RenameIndexOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateIndex(Index1Name, Column1Name);
            var operations = new[]
            {
                new RenameIndexOperation()
                {
                    Name = Index1Name,
                    NewName = Index1NewName,
                    Table = TableName
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesIndexExist(Index1Name));
            Assert.False(DoesIndexExist(Index1NewName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesIndexExist(Index1Name));
            Assert.True(DoesIndexExist(Index1NewName));
        }

        private void CreateIndex(string indexName, string columnName)
        {
            var operations = new[]
            {
                new CreateIndexOperation()
                {
                    Name = indexName,
                    Table = TableName,
                    Columns = new[] {columnName}
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreatePrimaryKey(string name)
        {
            var operations = new[]
            {
                new AddPrimaryKeyOperation
                {
                    Name = name,
                    Table = TableName,
                    Columns = new[] {Column1Name}
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreateForeignKey(string name)
        {
            CreatePrimaryKey(Column1Name);
            var operations = new[]
            {
                new AddForeignKeyOperation
                {
                    Name = name,
                    Table = TableName,
                    Columns = new[] {Column1Name},
                    PrincipalTable = TableName,
                    PrincipalColumns = new[] {Column1Name}
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreateUniqueConstraint(string name)
        {
            var operations = new[]
            {
                new AddUniqueConstraintOperation
                {
                    Name = name,
                    Table = TableName,
                    Columns = new[] {Column1Name}
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreateCheckConstraint(string name)
        {
            var operations = new[]
            {
                new AddCheckConstraintOperation
                {
                    Name = name,
                    Table = TableName,
                    Sql = $"{Column1Name} BETWEEN 1 AND 2"
                }
            };

            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private bool DoesPrimaryKeyExist(string name)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM USER_CONSTRAINTS " +
                                             $"WHERE CONSTRAINT_TYPE  = 'P' AND CONSTRAINT_NAME = '{name}'");
            return res != 0;
        }

        private bool DoesForeignKeyExist(string name)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM USER_CONSTRAINTS " +
                                             $"WHERE CONSTRAINT_TYPE  = 'R' AND CONSTRAINT_NAME = '{name}'");
            return res != 0;
        }

        private bool DoesUniqueConstraintExist(string name)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM USER_CONSTRAINTS " +
                                             $"WHERE CONSTRAINT_TYPE  = 'U' AND CONSTRAINT_NAME = '{name}'");
            return res != 0;
        }

        private bool DoesCheckConstraintExist(string name)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM USER_CONSTRAINTS " +
                                             $"WHERE CONSTRAINT_TYPE  = 'C' AND CONSTRAINT_NAME = '{name}'");
            return res != 0;
        }


        private bool DoesIndexExist(string indexName)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM user_indexes " +
                                             $"WHERE index_name = '{indexName}'");
            return res != 0;
        }

        #endregion

        #region Columns

        [Fact]
        public void Generate_AddColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AddColumnOperation
                {
                    Name = Column2Name,
                    Table = TableName,
                    ClrType = typeof(int),
                    ColumnType = "number"
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesColumnExist(Column2Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesColumnExist(Column2Name));
        }

        [Fact]
        public void Generate_AddRowVersionColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                CreateAddRowVersionOperation()
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            _testOutputHelper.WriteLine(commands[0].CommandText);
            Assert.False(DoesColumnExist(RowVersionColumnName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesColumnExist(RowVersionColumnName));
        }

        private static AddColumnOperation CreateAddRowVersionOperation()
        {
            return new()
            {
                Name = RowVersionColumnName,
                Table = TableName,
                ClrType = typeof(byte[]),
                ColumnType = "RAW(8)",
                IsRowVersion = true,
                IsNullable = false,
                DefaultValue = new byte[0]
            };
        }

        [Fact]
        public void Generate_AddRowVersionColumnOperation_AddsRowVersion()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                CreateAddRowVersionOperation()
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);

            //Act 
            Database.ExecuteSqlRaw($"INSERT INTO {TableName} ({Column1Name}) VALUES (1)");
            var initialTimestamp = ToInt64(ExecuteScalar<byte[]>($"SELECT {RowVersionColumnName} FROM {TableName}"));
            Database.ExecuteSqlRaw($"UPDATE {TableName} SET {Column1Name} = 2");

            //Assert
            var newTimestamp = ToInt64(ExecuteScalar<byte[]>($"SELECT {RowVersionColumnName} FROM {TableName}"));
            Assert.NotEqual(initialTimestamp, newTimestamp);
        }

        [Fact]
        public void Generate_DropColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateColumn(Column2Name);
            var operations = new[] {new DropColumnOperation {Name = Column2Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.True(DoesColumnExist(Column2Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesColumnExist(Column2Name));
        }

        [Fact]
        public void Generate_DropColumnOperation_DoesntDropRowVersion()
        {
            //Arrange
            ReCreateTestTable();
            CreateRowVersionColumn();
            CreateColumn(Column2Name);
            var operations = new[] {new DropColumnOperation {Name = Column2Name, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.True(DoesTriggerExist("rowversion_TEST_TABLE"));
            Assert.True(DoesColumnExist(Column2Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesColumnExist(Column2Name));
            Assert.True(DoesTriggerExist("rowversion_TEST_TABLE"));
        }

        [Fact]
        public void Generate_DropRowVersionColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            CreateRowVersionColumn();
            var operations = new[] {new DropColumnOperation {Name = RowVersionColumnName, Table = TableName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.True(DoesColumnExist(RowVersionColumnName));
            Assert.True(DoesTriggerExist("rowversion_TEST_TABLE"));
            _testOutputHelper.WriteLine(commands[1].CommandText);
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesColumnExist(RowVersionColumnName));
            Assert.False(DoesTriggerExist("rowversion_TEST_TABLE"));
        }

        [Fact]
        public void Generate_RenameColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new RenameColumnOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    NewName = Column2Name
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.True(DoesColumnExist(Column1Name));
            Assert.False(DoesColumnExist(Column2Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesColumnExist(Column2Name));
            Assert.False(DoesColumnExist(Column1Name));
        }

        [Fact]
        public void Generate_AlterColumnOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AlterColumnOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    ClrType = typeof(int)
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.True(DoesColumnExist(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesColumnExist(Column1Name));
        }

        private bool DoesColumnExist(string columnName)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM user_tab_columns " +
                                             $"WHERE table_name = '{TableName}' AND column_name = '{columnName}'");
            return res != 0;
        }

        private bool DoesTriggerExist(string triggerName)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM sys.all_triggers " +
                                             $"WHERE table_name = '{TableName}' AND trigger_name = '{triggerName}'");
            return res != 0;
        }

        private void CreateColumn(string columnName)
        {
            var operations = new[]
            {
                new AddColumnOperation
                {
                    Name = columnName,
                    Table = TableName,
                    ClrType = typeof(int),
                    ColumnType = "number"
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreateRowVersionColumn()
        {
            var operations = new[]
            {
                CreateAddRowVersionOperation()
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        #endregion

        #region Sequences

        [Fact]
        public void Generate_CreateSequenceOperation_IsIdempotent()
        {
            //Arrange
            DropSequence(SequenceName);
            var operations = new[]
            {
                new CreateSequenceOperation
                {
                    Name = SequenceName,
                    ClrType = typeof(int)
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesSequenceExist(SequenceName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesSequenceExist(SequenceName));
        }

        [Fact]
        public void Generate_DropSequenceOperation_IsIdempotent()
        {
            //Arrange
            CreateSequence(SequenceName);
            var operations = new[] {new DropSequenceOperation {Name = SequenceName}};

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesSequenceExist(SequenceName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesSequenceExist(SequenceName));
        }

        [Fact]
        public void Generate_RenameSequenceOperation_IsIdempotent()
        {
            //Arrange
            CreateSequence(SequenceName);
            DropSequence(NewSequenceName);
            var operations = new[]
            {
                new RenameSequenceOperation
                {
                    Name = SequenceName,
                    NewName = NewSequenceName
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesSequenceExist(SequenceName));
            Assert.False(DoesSequenceExist(NewSequenceName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.False(DoesSequenceExist(SequenceName));
            Assert.True(DoesSequenceExist(NewSequenceName));
        }

        [Fact]
        public void Generate_AlterSequenceOperation_IsIdempotent()
        {
            //Arrange
            CreateSequence(SequenceName);
            var operations = new[]
            {
                new AlterSequenceOperation
                {
                    Name = SequenceName,
                    MaxValue = 100,
                    MinValue = 0
                }
            };

            //Act 
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.True(DoesSequenceExist(SequenceName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesSequenceExist(SequenceName));
        }

        private void DropSequence(string name)
        {
            var operations = new[]
            {
                new DropSequenceOperation
                {
                    Name = name
                }
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private void CreateSequence(string name)
        {
            ExecuteOperation(new CreateSequenceOperation
            {
                Name = name,
                ClrType = typeof(int)
            });
        }

        #endregion

        #region Comments

        [Fact]
        public void Generate_CreateTableOperation_CommentIsSet()
        {
            //Arrange
            DropTestTable();
            var operations = new[]
            {
                new CreateTableOperation
                {
                    Name = TableName,
                    Comment = TableComment,
                    Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = Column1Name,
                            Table = TableName,
                            ClrType = typeof(int),
                            ColumnType = "number",
                            Comment = ColumnComment
                        }
                    }
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesTableExist(TableName));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesTableExist(TableName));
            Assert.Equal(TableComment, GetTableComment());
            Assert.Equal(ColumnComment, GetColumnComment(Column1Name));
        }

        [Fact]
        public void Generate_AddColumnOperation_CommentIsSet()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AddColumnOperation
                {
                    Name = Column2Name,
                    Table = TableName,
                    ClrType = typeof(int),
                    ColumnType = "number",
                    Comment = ColumnComment
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(1, commands.Count);
            Assert.False(DoesColumnExist(Column2Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.True(DoesColumnExist(Column2Name));
            Assert.Equal(ColumnComment, GetColumnComment(Column2Name));
        }

        [Fact]
        public void Generate_AlterTableOperation_CommentIsSet()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AlterTableOperation
                {
                    Name = TableName,
                    Comment = TableComment
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Null(GetTableComment());
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.Equal(TableComment, GetTableComment());
        }

        [Fact]
        public void Generate_AlterColumnOperation_CommentIsSet()
        {
            //Arrange
            ReCreateTestTable();
            var operations = new[]
            {
                new AlterColumnOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    ClrType = typeof(int),
                    Comment = ColumnComment
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Null(GetColumnComment(Column1Name));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.Equal(ColumnComment, GetColumnComment(Column1Name));
        }

        [Fact]
        public void Generate_InsertDataOperation_IsIdempotent()
        {
            //Arrange
            ReCreateTestTable();
            var values = new object[1, 1];
            values[0, 0] = 1;
            var operations = new[]
            {
                new InsertDataOperation()
                {
                    Table = TableName,
                    Columns = new[] {Column1Name},
                    ColumnTypes = new[] {"NUMBER"},
                    Values = values
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(0, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName} WHERE {Column1Name} = 1"));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.Equal(1, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName} WHERE {Column1Name} = 1"));
        }

        [Fact]
        public void Generate_InsertDataOperationWithNullColumn_IsIdempotent()
        {
            //Arrange
            DropTestTable();
            CreateTestTable(new AddColumnOperation
            {
                Name = Column1Name,
                Table = TableName,
                ClrType = typeof(int?),
                ColumnType = "number",
                IsNullable = true
            });
            var values = new object[1, 1];
            values[0, 0] = null;
            var operations = new[]
            {
                new InsertDataOperation()
                {
                    Table = TableName,
                    Columns = new[] {Column1Name},
                    ColumnTypes = new[] {"NUMBER"},
                    Values = values
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(0, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName} WHERE {Column1Name} is null"));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.Equal(1, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName} WHERE {Column1Name} is null"));
        }

        [Fact]
        public void Generate_InsertDataOperationMultipleColumns_IsIdempotent()
        {
            //Arrange
            DropTestTable();
            CreateTestTable(new AddColumnOperation
                {
                    Name = Column1Name,
                    Table = TableName,
                    ClrType = typeof(int),
                    ColumnType = "number"
                },
                new AddColumnOperation
                {
                    Name = Column2Name,
                    Table = TableName,
                    ClrType = typeof(int?),
                    ColumnType = "number",
                    IsNullable = true
                });
            var values = new object[1, 2];
            values[0, 0] = 1;
            values[0, 1] = null;
            var operations = new[]
            {
                new InsertDataOperation()
                {
                    Table = TableName,
                    Columns = new[] {Column1Name, Column2Name},
                    ColumnTypes = new[] {"NUMBER", "NUMBER"},
                    Values = values
                }
            };

            //Act
            var commands = _sqlGenerator.Generate(operations);

            //Assert
            Assert.Equal(0, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName}"));
            ExecuteCommands(commands);
            ExecuteCommands(commands);
            Assert.Equal(1, ExecuteScalar<decimal>($"SELECT COUNT(*) FROM {TableName}"));
        }

        private void ExecuteCommands(IReadOnlyList<MigrationCommand> commands)
        {
            foreach (var command in commands)
            {
                _testOutputHelper.WriteLine(command.CommandText);
                Database.ExecuteSqlRaw(command.CommandText);
            }
        }

        private string GetTableComment()
        {
            return ExecuteScalar<string>("SELECT COMMENTS FROM ALL_TAB_COMMENTS " +
                                         $"WHERE table_name = '{TableName}'");
        }

        private string GetColumnComment(string columnName)
        {
            return ExecuteScalar<string>("SELECT COMMENTS FROM ALL_COL_COMMENTS " +
                                         $"WHERE table_name = '{TableName}' AND column_name = '{columnName}'");
        }

        #endregion

        private void ExecuteOperation(MigrationOperation operation)
        {
            var operations = new[]
            {
                operation
            };
            var commands = _sqlGenerator.Generate(operations);
            ExecuteCommands(commands);
        }

        private bool DoesSequenceExist(string name)
        {
            var res = ExecuteScalar<decimal>("SELECT COUNT(*) FROM user_sequences " +
                                             $"WHERE sequence_name = '{name}'");
            return res != 0;
        }

        private T ExecuteScalar<T>(string sql)
        {
            var connection = Database.GetDbConnection();
            using var command = connection.CreateCommand();
            if (connection.State != System.Data.ConnectionState.Open) connection.Open();
            command.CommandText = sql;
            var result = command.ExecuteScalar();
            if (result == null || result is DBNull) return default;
            return (T) result;
        }
    }
}