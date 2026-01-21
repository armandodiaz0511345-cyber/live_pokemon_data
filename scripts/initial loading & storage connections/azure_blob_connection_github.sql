/*

Azure Blob Connection Script

Purpose: connects this sql server database the Azure blob servers through 


*/

--MUST BE CONNECTED TO DATABASE (live_data_pokemon)

-- 1. Create a Master Key for the database (if you haven't already)
-- This protects your secrets
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '[Your Database Password]';

-- 2. Create the Credential
-- Use the SAS token you copied, REMOVING the first '?' character
CREATE DATABASE SCOPED CREDENTIAL [pokemon_blob_cred]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '[Your Azure SAS]'; -- Paste your token here (no ?)

-- 3. Create the External Data Source
-- This acts as a permanent pointer to your folder
CREATE EXTERNAL DATA SOURCE [pokemon_blob_source]
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = '[container location in your blob storage]',
    CREDENTIAL = [pokemon_blob_cred]
);

/*
-- Replace 'pokemon_sample_20260111.json' with your actual filename

USE: DATE_SOURCE = 'pokemon_blob_source' within OPENROWSET, after 'Bulk'

SELECT * FROM OPENROWSET(
    BULK 'pokemon_sample_20260111.json',
    DATA_SOURCE = 'pokemon_blob_source',
    SINGLE_CLOB
) AS json_file;

*/

