/*
--==================================
--card json string NORMALIZATION SCRIPT (main)

--although the INITIAL purpose of this script was going to be to flatten every array and object,
--we're actually going to provide multiple rows for each ID, the number depending on the number of subtypes th pokemon has.

Idea is:

instead of 
SELECT * 
FROM Cards 
WHERE subtype1 = 'Stage 2' OR subtype2 = 'Stage 2' OR subtype3 = 'Stage 2' ...

Rather we:
SELECT * FROM Subtypes WHERE subtype_name = 'Stage 2' (seperate table)


--==================================
*/



--====SUBTYPES table (one ID to many subtypes)====--
--trim check, NULL check
SELECT 
c.id,
c.name,
s.value as subtype_name -- tak note of the '.value', we will be using it again
FROM bronze.cards_json_raw c
CROSS APPLY openjson(subtypes) as s
--all good



--====TYPES table (array) (one ID to many subtypes)====--
-- no nulls, no trims.
SELECT 
c.id,
c.name,
s.value as types_name -- tak note of the '.value', we will be using it again
FROM bronze.cards_json_raw c
CROSS APPLY openjson(types) as s
--all good

--====ATTACKS table (object)=======----
Select
id,
attack_name,
cost.value as energy_cost,
converted_energy_cost,
NULLIF(TRIM(damage),'') as damage,
text

FROM (
	SELECT
	c.id,
	a.value indv_atk_string
	FROM bronze.cards_json_raw c 
	CROSS APPLY openjson(attacks) as a
)t
CROSS APPLY openjson(indv_atk_string)
		WITH(
		attack_name				NVARCHAR(255) '$.name',
		cost					NVARCHAR(MAX) AS JSON,
		converted_energy_cost	INT '$.convertedEnergyCost',
		damage					NVARCHAR(255),
		text					NVARCHAR(MAX)
		) as indv_attack
CROSS APPLY openjson(cost) as cost



--Meta data--

--creation_date		DATETIME DEFAULT GETDATE() -- (DDL)