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
SELECT * FROM Subtypes WHERE subtype_name = 'Stage 2'


--==================================
*/

--NOTE: maybe consider fixing the (easy) 'first available' data from the main OPENJSON first, then nailing down into the ARRAYs and Tables.

--==ID==--
--count (uniqueness) check - different card have types of the same card have different IDs
--trim check

--all good
--==name==--
--note: name is not unique, cards are can be normal,ex, special rare,etc... but will always have the same name of pokemon
--trim check
--all good
--==supertype==--
--count check
--trim check
--all good
--==hp==--
--note trainers and energy dont ahve hp
--all good
--==evolves_from==--

--Recursive CTE -- for Tiering the evolutions
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

/*
SELECT
id
FROM (
SELECT 
id,
name,
tier,
ROW_NUMBER() OVER (partition by id ORDER BY id) iteration_number
from Evolutions
)t
WHERE iteration_number = 1

*/
)
/*
SELECT 
c.id,
name,
evolves_from
FROM bronze.cards_json_raw c
LEFT JOIN distinct_tiers d
ON c.id = d.id
Where d.id IS NULL

^^^^^ this is a check for the cards that were NOT included in the distinct query of the recursive CTE 
BECAUSE this is a sample DB, and their 'evolves_from' is not in the data!
*/

---


--==converted_retreat_cost==--
--going to convert Nulls to 0s, except for trainers and energies

/*Select 
name,
CASE WHEN supertype LIKE 'Pokémon' THEN Coalesce(converted_retreat_cost,0)
	else converted_retreat_cost
END as converted_retreat_cost
From bronze.cards_json_raw
*/


--==number==-- 
-- note this is the number of the card that the set is in.
/*Select

name,
CAST(TRANSLATE(lower(number),'abcdefghijklmnopqrstuvwxyz',REPLICATE(' ',26)) AS INT) set_number
From bronze.cards_json_raw
*/
--moved to set table
--==rarity==--
--==flavor_test==--




--Group by name
--order by count(name) desc

--====SUBTYPES (one ID to many subtypes)====--


/*


SELECT 
c.id,
s.value as subtype_name -- tak note of the '.value', we will be using it again
FROM bronze.cards_json_raw c
CROSS APPLY openjson(subtypes) as s



*/



--subtypes DATA INTEGRITY CHECKS--

--====EXTRA: SUBTYPES COUNT LIST====--
/*
SELECT 
s.value,
COUNT(s.value) as subtypes
FROM bronze.cards_json_raw c
CROSS APPLY openjson(subtypes) as s
GROUP BY s.value
order by COUNT(s.value) desc
*/


SELECT
c.id,
c.name as card_name,
supertype as supertype,
hp as hp,
evolution as evolution_tier, -- shows null for any 2nd or 3rd tier pokemon who's predecessors are not in the DB.
artist,
Coalesce(rarity,'n/a') as set_rarity,
Coalesce(flavor_text,'n/a') as description
FROM bronze.cards_json_raw as c
LEFT JOIN Evolutions as e
on c.id = e.id


