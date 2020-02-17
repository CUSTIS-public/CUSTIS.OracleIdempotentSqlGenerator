# Idempotent SQL Generator for Oracle

EF migrator isn't idempotent in case of using Oracle DB. Consider you have a migration which consists of 2 operations:
* create table MY_TABLE1;
* create table MY_TABLE2.
If the migration falls after 1-st statement, then Oracle DB will become inconclusive:
each DDL in Oracle commits the transaction, so DB will contain MY_TABLE1.
The migration could neither be run once more (as Oracle will fall on the attemp to create MY_TABLE1),
nor rolled back (EF "thinks" that migration is not installed and won't get it down).

Idempotent SQL Generator for Oracle generates code, that could be rerun in case of failure. 
For example, it generates such a code for creating MY_COLUMN in MY_TABLE:

    DECLARE
        i NUMBER;
    BEGIN
        SELECT COUNT(*) INTO i
        FROM user_tab_columns
        WHERE table_name = 'MY_TABLE' AND column_name = 'MY_COLUMN';
        IF I != 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE MY_TABLE ADD (MY_COLUMN DATE)';  
        END IF;       
    END;

P.S. EF with MS SQL is idempotent out of the box. 
MS SQL has transactional DDL, which causes the migration to be fully installed or rolled back in case of error.

# Running tests
* Open CMD at CUSTIS.OracleIdempotentSqlGenerator.Tests
* Run:
  * dotnet user-secrets set DB ****
  * dotnet user-secrets set Schema ****
  * dotnet user-secrets set Pwd ****    
* Tests will run at Schema@DB, which could be any schema (even empty)
