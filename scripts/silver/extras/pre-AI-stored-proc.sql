/*
======================================================
silver load scripts
purpose:

tables loaded:
======================================================
*/

CREATE PROCEDURE silver.load_silver AS
BEGIN

BEGIN TRY
--====LOAD silver.basic_card_info (central sheet)====---
PRINT '--====LOAD silver.card_main_info====---'
TRUNCATE TABLE silver.card_main_info;-- note CTEs expect an immediate 'SELECT' statement, so Truncations cannot be put between a CTE and it's main query.
--^^Trunc must include semi-colon or 'GO' if next line is a CTE or else it will throw an error.
--==silver.card_main_info + CTE==--
WITH Evolutions AS (
--anchor query
SELECT 
id,
name,
evolves_from,
1 as evolution
FROM bronze.cards_json_raw
WHERE evolves_from IS NULL

UNION ALL

SELECT 
c.id,
c.name,
c.evolves_from,
evolution +1
FROM bronze.cards_json_raw c
INNER JOIN Evolutions e
ON e.name = c.evolves_from
)
, distinct_tiers AS ( -- provides evolution #s to pokemons with 'evolved_from' that are visible in the database.
SELECT DISTINCT
id,
name,
evolution
from Evolutions
)

INSERT INTO silver.card_main_info (card_id,card_name,supertype,hp,evolution_tier,artist,set_card_rarity,description)
SELECT DISTINCT
c.id,
LOWER(c.name) as card_name,
LOWER(supertype) as supertype,
CAST(hp as INT) as hp,
evolution as evolution_tier, -- shows null for any 2nd or 3rd tier pokemon who's predecessors are not in the DB.
LOWER(artist) as artist,
LOWER(Coalesce(rarity,'special')) as set_card_rarity,
LOWER(Coalesce(flavor_text,'n/a')) as description
FROM bronze.cards_json_raw as c
LEFT JOIN Evolutions as e
on c.id = e.id

--===========================================
--ARRAY Tables (card sub-attributes)
--===========================================

--====LOAD silver.card_attacks (array sheet)====---
TRUNCATE TABLE silver.card_attacks
PRINT '--====LOAD silver.card_attacks====---'
INSERT INTO silver.card_attacks (card_id,attack_name,energy_needed,energy_amount_neeed,total_energy_cost,damage,description)
SELECT DISTINCT
id, --character id
lower(attack_name),
lower(energy_needed),
COUNT(energy_needed) OVER(PARTITION BY id,attack_name,energy_needed) energy_amount_needed,
converted_energy_cost,
COALESCE(NULLIF(TRIM(damage),''),'n/a') as damage,
COALESCE(NULLIF(TRIM(text),''),'n/a') as description
FROM(Select
id as id,
attack_name as attack_name,
cost.value as energy_needed,
converted_energy_cost as converted_energy_cost,
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
)tt

--====LOAD silver.card_weaknesses (array sheet)====---
PRINT '--====LOAD silver.card_weaknesses====---'

SELECT
c.id,
lower(wa.type) weakness_type,
wa.value weakness_value
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.weaknesses) as w
CROSS APPLY OPENJSON(w.value)
			WITH(
			type NVARCHAR(200),
			value NVARCHAR(200)
			) as wa

--====LOAD silver.card_abilities (array sheet)====---
PRINT '--====LOAD silver.card_abilities====---'

SELECT
c.id, --fk
--c.name as card_name,
lower(ability.name) as ability_name,
--ability.type, -- not including this as there are historical equal, and nowadays they are all just 'Ability'
ability.text as ability_description
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.abilities) a
CROSS APPLY OPENJSON(a.value)
			WITH(
			name NVARCHAR(255),
			type NVARCHAR(255),
			text NVARCHAR(MAX)
			) as ability

--====LOAD silver.card_resistances (array sheet)====---
PRINT '--====LOAD silver.card_resistances====---'

SELECT
c.id,--fk
lower(ra.type) resistances_type,
ra.value resistances_value
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.resistances) as r
CROSS APPLY OPENJSON(r.value)
			WITH(
			type NVARCHAR(200),
			value NVARCHAR(200)
			) as ra

--====LOAD silver.card_retreat_costs (array sheet)====---
PRINT '--====LOAD silver.card_retreat_costs====---'

SELECT
c.id,--fk
lower(rc.value) energy_type_to_retreat,
CASE WHEN supertype LIKE 'Pokémon' THEN Coalesce(converted_retreat_cost,0)
	else converted_retreat_cost
END as converted_retreat_cost
--count(rc.value) amount_needed
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.retreat_cost) rc

--====LOAD silver.card_subtypes (array sheet)====---
PRINT '--====LOAD silver.card_subtypes====---'

SELECT 
c.id,
--c.name,
lower(s.value) as subtype_name -- tak note of the '.value', we will be using it again
FROM bronze.cards_json_raw c
CROSS APPLY openjson(subtypes) as s

--====LOAD silver.card_types (array sheet)====---
PRINT '--====LOAD silver.card_types====---'

SELECT 
c.id,
--c.name as card_name,
lower(s.value) as types_name -- tak note of the '.value', we will be using it again
FROM bronze.cards_json_raw c
CROSS APPLY openjson(types) as s

--===========================================
--Object Tables (card info extensions)
--===========================================

--====LOAD silver.card_set (central sheet)====---
PRINT '--====LOAD silver.card_set====---'

SELECT DISTINCT-- all distinct sets available in data set -- must make a Foreign key for in main table
c.id,-- use this to initially join to MAIN table (in gold layer) and then put a set id in the main table, as an FK for this table.
s.id as set_id, -- **join main table w/ this and make a set id in the main table (fk fo rthis table).
CAST(TRANSLATE(lower(number),'abcdefghijklmnopqrstuvwxyz',REPLICATE(' ',26)) AS INT) card_set_number,
LOWER(s.name) as set_name,
LOWER(s.series) as set_series,
s.printed_total as set_total_printed,
s.total as set_total_cards,
s.ptcgo_code as set_ptcgo_code,
s.release_date as set_release_date, -- yyyy/mm/dd
s.updated_at as set_update_date,-- when set was updated (by pokemon) yyyy/mm/dd
images.symbol_url as set_symbol_image,
images.logo_url as set_logo_image
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.[set]) 
			WITH(
			id NVARCHAR(50),
			name NVARCHAR(255),
			series NVARCHAR(255),
			printed_total INT '$.printedTotal',
			total INT,
			ptcgo_code NVARCHAR(255) '$.ptcgoCode',
			release_date DATETIME '$.releaseDate',
			updated_at DATETIME2 '$.updatedAt',
			images NVARCHAR(MAX) AS JSON
			) as s
			CROSS APPLY OPENJSON(s.images)
						WITH(
						symbol_url NVARCHAR(500) '$.symbol',
						logo_url NVARCHAR(500) '$.logo'
						) as images

--====LOAD silver.card_national_pokedex_numbers (central sheet)====---
PRINT '--====LOAD silver.card_national_pokedex_numbers====---'

SELECT
c.id,
--c.name,
CAST(npn.value AS INT) national_pokedex_number -- casting as an int because we havent done it yet (this was an nvarchar since we just too it out of a json file).
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.national_pokedex_numbers) npn


--====LOAD silver.card_legalities (central sheet)====---
PRINT '--====LOAD silver.card_legalities====---'

SELECT
c.id,
--commenting out for better CASE WHEN statement below.
/*COALESCE(l.standard,'Not Legal') as legality_standard,
COALESCE(l.expanded,'Not Legal') as legality_expanded,
l.unlimited as legality_unlimited,
*/
CASE WHEN l.standard IS NOT NULL THEN 'Standard+'
	 WHEN l.expanded IS NOT NULL THEN 'Expanded+'
	 WHEN l.unlimited IS NOT NULL THEN 'Unlimited Only'
	 else 'n/a'
end legality
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.legalities)
			WITH(
			standard	NVARCHAR(50),
			unlimited	NVARCHAR(50),
			expanded	NVARCHAR(50)
			) as l


--====LOAD silver.card_images (central sheet)====---
PRINT '--====LOAD silver.card_images====---'

SELECT
c.id,
im.small as small_image_url,
im.large as large_image_url
FROM bronze.cards_json_raw c
CROSS APPLY OPENJSON(c.images)
			WITH(
			small NVARCHAR(500),
			large NVARCHAR(500)
			)as im

--====LOAD silver.mkt_tcgplayer (central sheet)====---
PRINT '--====LOAD silver.mkt_tcgplayer====---'

SELECT
id,
--name,
printing_finish_rarity as printing_finish_rarity,
tcg_updated_at,--yyyy/mm/dd
Coalesce(market, (high+low)/2) as market_price,
high as highest_price,
low as lowest_price,
direct_low as direct_low_price, -- likely not useful, but handle in gold tier.
tcg_url
FROM(SELECT
c.id,
c.name,
p.[key] as printing_finish_rarity,
CAST(JSON_VALUE(p.[value],'$.market') AS float) as market,
CAST(JSON_VALUE(p.[value],'$.low') AS float) as low,
CAST(JSON_VALUE(p.[value],'$.mid') AS float) as mid,
CAST(JSON_VALUE(p.[value],'$.high') AS float) as high,
CAST(JSON_VALUE(p.[value],'$.directLow') AS float) as direct_low,
tcg.updated_at as tcg_updated_at,
tcg.url as tcg_url
FROM bronze.cards_json_raw as c
CROSS APPLY OPENJSON(c.tcgplayer)
			WITH(
			url NVARCHAR(255),
			updated_at DATE '$.updatedAt',
			prices NVARCHAR(MAX) AS JSON -- remember the AS JSON!
			) as tcg
CROSS APPLY OPENJSON(tcg.prices) as p
Where updated_at > GETDATE() - 20 -- assure recency
)t

--====LOAD silver.mkt_cardmarket (central sheet)====---
PRINT '--====LOAD silver.mkt_cardmarket====---'

SELECT
c.id,
crd.updated_at as cardmarket_updated_at, --yyyy/mm/dd
p.average_sell_price,
p.low_price,
p.trend_price,
--p.german_pro_low, out for now (all zeros)
--p.suggested_price, out for now (all zeros)
p.reverse_holo_sell,
p.reverse_holo_low,
p.reverse_holo_trend,
p.low_price_ex_plus,
p.avg_1 as avg_1_day,
p.avg_7 as avg_7_day,
p.avg_30 as avg_30_day,
p.reverse_holo_avg_1 as reverse_holo_avg_1_day,
p.reverse_holo_avg_7 as reverse_holo_avg_7_day,
p.reverse_holo_avg_30 as reverse_holo_avg_30_day,
crd.url as card_url
FROM bronze.cards_json_raw as c
CROSS APPLY OPENJSON(cardmarket)
			WITH(
			url NVARCHAR(255),
			updated_at DATE '$.updatedAt',
			prices NVARCHAR(MAX) AS JSON
			) as crd
CROSS APPLY OPENJSON(prices)
			WITH (
    average_sell_price  FLOAT '$.averageSellPrice',
    low_price           FLOAT '$.lowPrice',
    trend_price         FLOAT '$.trendPrice',
    german_pro_low      FLOAT '$.germanProLow',
    suggested_price     FLOAT '$.suggestedPrice',
    reverse_holo_sell   FLOAT '$.reverseHoloSell',
    reverse_holo_low    FLOAT '$.reverseHoloLow',
    reverse_holo_trend  FLOAT '$.reverseHoloTrend',
    low_price_ex_plus   FLOAT '$.lowPriceExPlus',
    avg_1               FLOAT '$.avg1',
    avg_7               FLOAT '$.avg7',
    avg_30              FLOAT '$.avg30',
    reverse_holo_avg_1  FLOAT '$.reverseHoloAvg1',
    reverse_holo_avg_7  FLOAT '$.reverseHoloAvg7',
    reverse_holo_avg_30 FLOAT '$.reverseHoloAvg30'
) AS p

END TRY

BEGIN CATCH


END CATCH


END