-- https://stackoverflow.com/questions/57949912/postgres-roles-and-users-permission-denied-for-table
-- Revoke privileges from 'public' role
REVOKE CREATE ON SCHEMA information_schema
FROM
    PUBLIC;
REVOKE ALL ON DATABASE postgres
FROM
    PUBLIC;
-- Read-only role
CREATE ROLE visitor LOGIN PASSWORD '123';
GRANT CONNECT ON DATABASE postgres TO visitor;
GRANT USAGE ON SCHEMA information_schema TO visitor;
GRANT
SELECT
    ON ALL TABLES IN SCHEMA information_schema TO visitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA information_schema GRANT
SELECT
    ON TABLES TO visitor;
CREATE ROLE administrator SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD '123';
--to check out
--psql -U user1 retailanalitycs
--SET search_path TO myschema;
--select * from newtable;
--REASSIGN OWNED BY visitor TO postgres;
--DROP OWNED BY visitor;
--drop role visitor;