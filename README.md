# Idempotent SQL Generator for Oracle

# Running tests
* Open CMD in the folder of CUSTIS.OracleIdempotentSqlGenerator.Tests
* Run:
  * dotnet user-secrets set DB ****
  * dotnet user-secrets set Schema ****
  * dotnet user-secrets set Pwd ****
* Tests will be run at Schema@DB, which could be any schema (even empty)