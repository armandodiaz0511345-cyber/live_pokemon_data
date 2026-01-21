
/*
===============================
bronze DYNAMIC card Load

Purpose: Creates+ Inserts all JSON File data into the bronze layer for today's API pull.
-- Gathers Data on all pokemon, grouped by simple attributes, array attributes, and object attributes.
--Uses OPENROWSET to reference (something) outside of the dataset
-- Uses Bulk to point to today's file -- THIS IS DYNAMIC, Changes with today's date  (see variable @sql_executable (uses REPLACE function for daily files))
-- Uses Single_clob to pull entire Bulkcolumn.
-- uses OPENJSON() WITH() for BulkColumn & data classification.

-- utilizes sp_executesql @[variable] to create dynamic loads. (executes a given string as an sql.
===============================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

--DYNAMIC date card extraction into bronze layer

DECLARE @sql_extract_format NVARCHAR(MAX);
DECLARE @sql_executable NVARCHAR(MAX);
DECLARE @start_time DATETIME2;
DECLARE @end_time DATETIME2;


SET @sql_extract_format = N'
SELECT 
		cards.*,
	   GETDATE() as load_date --meta data for Historical Analysis --data type: datetime
INTO bronze.cards_json_raw
FROM OPENROWSET(
	BULK ''{{filename}}''
	,DATA_SOURCE = ''pokemon_blob_source''
	,SINGLE_CLOB  -- UTF-8
	,CODEPAGE = ''65001'') as sample_data -- CODEPAGE 65001 needed to handle proper unicode.
CROSS APPLY OPENJSON(BulkColumn)
			WITH(
			--Simple attributes -- information is not enclosed
			id NVARCHAR(50),
			name NVARCHAR(255),
			supertype			NVARCHAR(255),
			hp					INT,
			evolves_from		NVARCHAR(255) ''$.evolvesFrom'',
			converted_retreat_cost INT ''$.convertedRetreatCost'',
			number				NVARCHAR(MAX),
			artist				NVARCHAR(255),
			rarity				NVARCHAR(255),
			flavor_text			NVARCHAR(MAX) ''$.flavorText'',

			--Arrays -- information is in []
			subtypes			NVARCHAR(MAX) AS JSON,
			types				NVARCHAR(MAX) AS JSON,
			attacks				NVARCHAR(MAX) AS JSON,
			weaknesses			NVARCHAR(MAX) AS JSON,
			resistances			NVARCHAR(MAX) AS JSON,
			retreat_cost		NVARCHAR(MAX) ''$.retreatCost'' AS JSON,
			national_pokedex_numbers NVARCHAR(MAX) ''$.nationalPokedexNumbers'' AS JSON,

			
			--Objects -- information is in {}
			[set]				NVARCHAR(MAX) AS JSON,
			abilities			NVARCHAR(MAX) AS JSON,
			legalities			NVARCHAR(MAX) AS JSON,
			images				NVARCHAR(MAX) AS JSON,
			tcgplayer			NVARCHAR(MAX) AS JSON,
			cardmarket			NVARCHAR(MAX) AS JSON
			) as cards'
SET @sql_executable = REPLACE(@sql_extract_format,'{{filename}}',CONCAT('pokemon_data_',CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR(8)),'.json'))

PRINT @sql_executable

BEGIN TRY

SET @start_time = GETDATE()
PRINT'=============================='
PRINT'BEGINNING bronze load'
PRINT'=============================='


IF OBJECT_ID('bronze.cards_json_raw','U') IS NOT NULL
	DROP TABLE bronze.cards_json_raw;

EXEC sp_executesql @sql_executable;

SET @end_time = GETDATE()

PRINT'=============================='
PRINT'Time to Load Bronze: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+' Seconds'
PRINT'=============================='


END TRY

BEGIN CATCH
 PRINT 'ERROR MESSAGE: '+ERROR_MESSAGE()
 PRINT 'ERROR NUMBER: '+ CAST(ERROR_NUMBER() as NVARCHAR(100))

END CATCH
END

