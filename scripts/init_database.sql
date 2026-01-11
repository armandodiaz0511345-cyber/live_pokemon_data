/*
Simply Creating the database and Schemas
*/
USE master;
Go
-- check if db already exists

IF EXISTS (SELECT 1 from sys.databases WHERE name = 'live_data_pokemon')
BEGIN
  ALTER DATABASE live_data_pokemon SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE live_data_pokemon;
END;
GO

-- create new database

CREATE DATABASE live_data_pokemon;
GO

USE live_data_pokemon;
GO



CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
