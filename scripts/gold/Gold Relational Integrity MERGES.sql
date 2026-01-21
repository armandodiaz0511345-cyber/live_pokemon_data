/*========================================

GOLD DIMENSIONAL MERGES STORED PROCEDURE

Purpose: create SCD (slowly changing Dimensions) for:
1) gold.card_sets
2) gold.cards
2) gold.cards_extra_info

gold.card_sets and gold.cards are (1:Many),
gold.cards and cards_extra_info are (1:1),
but only at the expense of using String aggregations subqueries for the extra information
-- hence why the 'gold.dim_cards_extra_info is so slow.
-- tips?

========================================*/





CREATE OR ALTER PROCEDURE gold.load_dim_cards AS


BEGIN
DECLARE @start_time_total DATETIME
DECLARE @end_time_total DATETIME
DECLARE @merge_start_time DATETIME
DECLARE @merge_end_time DATETIME


SET @start_time_total = GETDATE();
SET @merge_start_time = GETDATE();
--=================================

--SET DIMENSION TABLE

--=================================
MERGE gold.dim_card_sets as target
USING (
SELECT DISTINCT
set_id,
set_name,
set_series,
set_total_printed,
set_total_cards,
set_release_date,
set_update_date
FROM silver.card_set
WHERE [row_number] =1
) as source
ON (target.set_id = source.set_id)

WHEN MATCHED THEN
UPDATE SET 
	--target.set_id = source.set_id, -- doesnt need to be updated, as it is used as the main mapping key (DDL IDENTITY (1,1)
	target.set_name = source.set_name,
	target.set_series = source.set_series,
	target.set_total_printed = source.set_total_printed,
	target.set_total_cards = source.set_total_cards,
	target.set_release_date = source.set_release_date,
	target.set_update_date = source.set_update_date,
	target.load_time_stamp = SYSDATETIME()

WHEN NOT MATCHED BY TARGET THEN
INSERT (set_id, set_name, set_series, set_total_printed, set_total_cards, set_release_date, set_update_date)
VALUES (source.set_id, source.set_name, source.set_series, source.set_total_printed,source.set_total_cards, source.set_release_date, source.set_update_date);

SET @merge_end_time = GETDATE();

PRINT'============================='
PRINT'Time to load card_sets table: '+ CAST(DATEDIFF(second, @merge_start_time,@merge_end_time) AS NVARCHAR(10))+' seconds.'
PRINT'============================='

SET @merge_start_time =GETDATE();
--=================================

--MAIN CARD INFO DIMENSION TABLE

--=================================
MERGE gold.dim_cards as target
USING (
SELECT DISTINCT
cm.*,
l.legality,
CASE WHEN cm.supertype IN ('trainer','pokémon') THEN COALESCE(att.number_of_attacks,0) else att.number_of_attacks END as number_of_attacks,
CASE WHEN cm.supertype IN ('trainer','pokémon') THEN COALESCE(abt.number_of_abilities,0) else abt.number_of_abilities END as number_of_abilities,
gold_set.set_key
FROM silver.card_main_info cm
LEFT JOIN silver.card_legalities l
ON cm.card_id = l.card_id
LEFT JOIN (
SELECT
card_id,
COUNT(attack_name) as number_of_attacks
FROM silver.card_attacks
GROUP BY card_id
) att
ON cm.card_id = att.card_id 
LEFT JOIN (
SELECT
card_id,
COUNT(ability_name) as number_of_abilities
FROM silver.card_abilities
GROUP BY card_id
) abt
ON cm.card_id = abt.card_id
LEFT JOIN silver.card_set as s
ON cm.card_id = s.card_id
LEFT JOIN gold.dim_card_sets as gold_set
on s.set_id = gold_set.set_id

) as source
ON (target.card_id = source.card_id)

WHEN MATCHED THEN
UPDATE SET -- note: card_key not updated, as it is created by the DDL IDENTITY (1,1)
	target.set_key	 = source.set_key, -- this needs to get a reference to the silver and then gold set table for proper granulation
	target.card_name = source.card_name,
	target.supertype = source.supertype,
	target.evolution_tier = source.evolution_tier,
	target.artist = source.artist,
	target.set_card_rarity = source.set_card_rarity,
	target.set_card_number = source.set_card_number, -- this will be in the silver, dont worry about this
	target.legality	= source.legality,
	target.number_of_attacks = source.number_of_attacks,
	target.number_of_abilities = source.number_of_abilities,
	target.hp		 = source.hp,
	target.load_time_stamp = SYSDATETIME()

WHEN NOT MATCHED BY TARGET THEN
INSERT (set_key, card_id, card_name, supertype, evolution_tier, artist, set_card_rarity, set_card_number, legality, number_of_attacks, number_of_abilities,hp)
VALUES (source.set_key, source.card_id, source.card_name, source.supertype, source.evolution_tier, source.artist, source.set_card_rarity, source.set_card_number, source.legality, source.number_of_attacks, source.number_of_abilities, source.hp);

SET @merge_end_time = GETDATE();

PRINT'============================='
PRINT'Time to load cards table: '+ CAST(DATEDIFF(second, @merge_start_time,@merge_end_time) AS NVARCHAR(10))+' seconds.'
PRINT'============================='

SET @merge_start_time =GETDATE();

--=================================

--EXTRAS DIMENSION TABLE

--=================================
MERGE gold.dim_cards_extra_info as target
USING (

SELECT DISTINCT
c.card_key,----- the string aggregations below need to be broken out individially.
--st.subtype,
COALESCE((SELECT STRING_AGG(subtype,', ')FROM silver.card_subtypes WHERE card_id = c.card_id),'n/a') as subtype,
COALESCE((SELECT STRING_AGG(type,', ')FROM silver.card_types WHERE card_id = c.card_id),'n/a') as type,
COALESCE((SELECT STRING_AGG(resistance_type,', ')FROM silver.card_resistances WHERE card_id = c.card_id),'n/a') as resistance_type,
--COALESCE(rs.resistance_value, 'n/a') resistance_value,
COALESCE((SELECT STRING_AGG(weakness_type,', ')FROM silver.card_weaknesses WHERE card_id = c.card_id),'n/a') as weakness_type,
--COALESCE(w.weakness_value,'n/a') weakness_value,
COALESCE((SELECT STRING_AGG(national_pokedex_number,', ')FROM silver.card_national_pokedex_numbers WHERE card_id = c.card_id),'n/a') as national_pokedex_number,
i.small_image_url,
i.large_image_url
FROM gold.dim_cards c
--LEFT JOIN silver.card_types t ON c.card_id = t.card_id
--LEFT JOIN silver.card_resistances rs ON c.card_id = rs.card_id
--LEFT JOIN silver.card_weaknesses w ON c.card_id = w.card_id
--LEFT JOIN silver.card_national_pokedex_numbers p ON c.card_id =p.card_id
LEFT JOIN silver.card_images i ON c.card_id = i.card_id
--LEFT JOIN silver.card_subtypes st ON c.card_id = st.card_id

) as source
ON (target.card_key = source.card_key) -- have to do multiple because i dont want to make a surrogate 'extra_info_key' since this will likely not get used 99% of the time.
WHEN MATCHED THEN
UPDATE SET
target.card_key = source.card_key,
target.subtype = source.subtype,
target.type = source.type,
target.resistance_type = source.resistance_type,
--target.resistance_value = source.resistance_value,
target.weakness_type = source.weakness_type,
--target.weakness_value = source.weakness_value,
target.national_pokedex_number = source.national_pokedex_number,
target.small_image_url = source.small_image_url,
target.large_image_url = source.large_image_url


WHEN NOT MATCHED BY TARGET THEN
INSERT (card_key,subtype,type,resistance_type,weakness_type,national_pokedex_number,small_image_url,large_image_url)
VALUES (source.card_key,source.subtype,source.type,source.resistance_type,source.weakness_type,source.national_pokedex_number,source.small_image_url,large_image_url);

END

SET @merge_end_time = GETDATE();

PRINT'============================='
PRINT'Time to load cards_extra_info table: '+ CAST(DATEDIFF(second, @merge_start_time,@merge_end_time) AS NVARCHAR(10))+' seconds.'
PRINT'============================='

SET @end_time_total = GETDATE();

PRINT'============================='
PRINT'Total Merge Time: '+ CAST(DATEDIFF(second, @start_time_total,@end_time_total) AS NVARCHAR(10))+' seconds.'
PRINT'============================='



/*
=================TESTING=================

SELECT * from gold.dim_card_sets
SELECT * from gold.dim_cards
where supertype = 'trainer'

SELECT set_id, COUNT(*) 
FROM (SELECT DISTINCT set_id, set_name, set_series, set_total_printed, set_total_cards, set_release_date, set_update_date FROM silver.card_set) t
GROUP BY set_id
HAVING COUNT(*) > 1;


SELECT 
card_key, subtype, type,
COUNT (type)
FROM gold.dim_cards_extra_info
GROUP BY card_key, subtype, type
order by count(type) desc

SELECT 
    card_id, 
    STRING_AGG(type, ', ') AS all_types
FROM silver.card_types
GROUP BY card_id
*/


