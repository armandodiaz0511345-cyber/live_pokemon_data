

/*

=======================
***NOTE: This script is only works for the date SPECIFIED IN THE OPENROWSET( BULK _____, SINGLE_CLOB)***
===========================

===============================
bronze_card_load
Purpose: Creates+ Inserts all JSON File data into the bronze layer for today's API pull.
-- Gathers Data on all pokemon, grouped by simple attributes, array attributes, and object attributes.
--Uses OPENROWSET to reference (something) outside of the dataset
-- Uses Bulk to point to today's file
-- Uses Single_clob to pull entire Bulkcolumn.
-- uses OPENJSON() WITH() for BulkColumn & data claassification.
===============================

*/
IF OBJECT_ID('bronze.cards_json_raw','U') IS NOT NULL
	DROP TABLE bronze.cards_json_raw

SELECT 
		cards.*,
	   GETDATE() as load_date --meta data for Historical Analysis --data type: datetime

INTO bronze.cards_json_raw
FROM OPENROWSET(
	BULK 'pokemon_sample_20260111.json'
	,DATA_SOURCE = 'pokemon_blob_source'
	,SINGLE_CLOB  -- UTF-8
	,CODEPAGE = '65001') as sample_data -- CODEPAGE 65001 needed to handle proper unicode.
CROSS APPLY OPENJSON(BulkColumn)
			WITH(
			--Simple attributes -- information is not enclosed
			id NVARCHAR(50),
			name NVARCHAR(255),
			supertype			NVARCHAR(255),
			hp					INT,
			evolves_from		NVARCHAR(255) '$.evolvesFrom',
			converted_retreat_cost INT '$.convertedRetreatCost',
			number				NVARCHAR(MAX),
			artist				NVARCHAR(255),
			rarity				NVARCHAR(255),
			flavor_text			NVARCHAR(MAX) '$.flavorText',

			--Arrays -- information is in []
			subtypes			NVARCHAR(MAX) AS JSON,
			types				NVARCHAR(MAX) AS JSON,
			attacks				NVARCHAR(MAX) AS JSON,
			weaknesses			NVARCHAR(MAX) AS JSON,
			resistances			NVARCHAR(MAX) AS JSON,
			retreat_cost		NVARCHAR(MAX) '$.retreatCost' AS JSON,
			national_pokedex_numbers NVARCHAR(MAX) '$.nationalPokedexNumbers' AS JSON,

			
			--Objects -- information is in {}
			[set]				NVARCHAR(MAX) AS JSON,
			abilities			NVARCHAR(MAX) AS JSON,
			legalities			NVARCHAR(MAX) AS JSON,
			images				NVARCHAR(MAX) AS JSON,
			tcgplayer			NVARCHAR(MAX) AS JSON,
			cardmarket			NVARCHAR(MAX) AS JSON
			) as cards

GO


--SELECT * FROM bronze.cards_json_raw



--=====================================
--======================================



--Insert Data example--

--example
/*
SELECT *
FROM OPENROWSET(BULK 'C:\live_data_pokemon\scripts\json example data.json', SINGLE_CLOB) as J
CROSS APPLY OPENJSON(BulkColumn)
WITH 
(
id NVARCHAR(50),
name NVARCHAR(255),
supertype NVARCHAR(50)
) as pokemon
*/


