/*
======================================================
silver.load_silver
Purpose: Transform data from Bronze (JSON) to Silver (Relational).
This procedure performs a full refresh (TRUNCATE + INSERT) every day, keeping all clean, up-to-date data in the silver layer.
======================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON; -- prevents sql from sending back 'rows affected' message

    -- Variables for timing
    DECLARE @start_time DATETIME, @end_time DATETIME;
    SET @start_time = GETDATE();

    BEGIN TRY
        PRINT '---------------------------------------------------';
        PRINT 'Starting Silver Layer Load: ' + CAST(@start_time AS VARCHAR);
        PRINT '---------------------------------------------------';


        --==== 2. silver.card_attacks ====---
        PRINT '--==== LOAD silver.card_attacks ====---';
        TRUNCATE TABLE silver.card_attacks;
        INSERT INTO silver.card_attacks (card_id, attack_name, energy_needed, energy_amount_neeed, total_energy_cost, damage, description)
        SELECT 
            id as card_id,
            lower(attack_name) as attack_name,
            lower(energy_needed) as energy_needed,
            COUNT(energy_needed) OVER(PARTITION BY id, attack_name, energy_needed) as energy_amount_needed,
            converted_energy_cost as total_energy_cost,
            COALESCE(NULLIF(TRIM(damage),''),'n/a') as damage,
            COALESCE(NULLIF(TRIM(text),''),'n/a') as description
        FROM (
            SELECT c.id, indv_attack.*, cost.value as energy_needed
            FROM bronze.cards_json_raw c 
            CROSS APPLY OPENJSON(attacks) WITH (
                attack_name NVARCHAR(255) '$.name',
                cost NVARCHAR(MAX) AS JSON,
                converted_energy_cost INT '$.convertedEnergyCost',
                damage NVARCHAR(255),
                text NVARCHAR(MAX)
            ) as indv_attack
            CROSS APPLY OPENJSON(indv_attack.cost) as cost
        ) t;

        --==== 3. silver.card_weaknesses ====---
        PRINT '--==== LOAD silver.card_weaknesses ====---';
        TRUNCATE TABLE silver.card_weaknesses;
        INSERT INTO silver.card_weaknesses (card_id, weakness_type, weakness_value)
        SELECT c.id as card_id,
        LOWER(wa.type) as weakness_type, 
        wa.value as weakness_value
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.weaknesses) WITH (type NVARCHAR(50), value NVARCHAR(50)) as wa;

        --==== 4. silver.card_abilities ====---
        PRINT '--==== LOAD silver.card_abilities ====---';
        TRUNCATE TABLE silver.card_abilities;
        INSERT INTO silver.card_abilities (card_id, ability_name, ability_description)
        SELECT c.id as card_id,
        LOWER(a.name) as ability_name,
        a.text as ability_description
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.abilities) WITH (name NVARCHAR(100), text NVARCHAR(2000)) as a;

        --==== 5. silver.card_resistances ====---
        PRINT '--==== LOAD silver.card_resistances ====---';
        TRUNCATE TABLE silver.card_resistances;
        INSERT INTO silver.card_resistances (card_id, resistance_type, resistance_value)
        SELECT c.id as card_id,
        LOWER(ra.type) as resistance_type,
        ra.value as resistance_value
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.resistances) WITH (type NVARCHAR(50), value NVARCHAR(50)) as ra;

        --==== 6. silver.card_retreat_cost ====---
        PRINT '--==== LOAD silver.card_retreat_cost ====---';
        TRUNCATE TABLE silver.card_retreat_cost;
        INSERT INTO silver.card_retreat_cost (card_id, energy_type_to_retreat)
        SELECT c.id as card_id,
        LOWER(rc.value) as energy_type_to_retreat
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.retreat_cost) rc;

        --==== 7. silver.card_subtypes ====---
        PRINT '--==== LOAD silver.card_subtypes ====---';
        TRUNCATE TABLE silver.card_subtypes;
        INSERT INTO silver.card_subtypes (card_id, subtype)
        SELECT c.id as card_id,
        LOWER(s.value) as subtype
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(subtypes) as s;

        --==== 8. silver.card_types ====---
        PRINT '--==== LOAD silver.card_types ====---';
        TRUNCATE TABLE silver.card_types;
        INSERT INTO silver.card_types (card_id, [type])
        SELECT c.id as card_id,
        LOWER(s.value) as [type]
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(types) as s;

        --==== 9. silver.card_set ====---
        PRINT '--==== LOAD silver.card_set ====---';
        TRUNCATE TABLE silver.card_set;
        WITH ranked_sets AS(
        SELECT DISTINCT
            --DENSE_RANK() OVER(ORDER BY s.id) as set_key,-- this is done in GOLD layer (surrogates, interconnections through silver)
            c.id as card_id,
            s.id as set_id,
            CAST(TRANSLATE(LOWER(c.number),'abcdefghijklmnopqrstuvwxyz!?',REPLICATE(' ',28)) AS INT) set_card_number,
            LOWER(s.name) as set_name,
            LOWER(s.series) as set_series,
            s.printed_total as set_printed_total,
            s.total as set_total_cards,
            s.ptcgo_code as set_ptcgo_code,
            s.release_date as set_release_date,
            CASE WHEN LEN(s.updated_at) != 19 THEN CAST(CONCAT('2',s.updated_at) as DATETIME)
                 ELSE CAST(s.updated_at as DATETIME)
            END as set_update_date,
            images.symbol_url as set_symbol_image,
            images.logo_url as set_logo_image,
            ROW_NUMBER() OVER(PARTITION BY s.id ORDER BY CASE WHEN LEN(s.updated_at) != 19 THEN CAST(CONCAT('2',s.updated_at) as DATETIME) ELSE CAST(s.updated_at as DATETIME) 
            END ) as row_num
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.[set]) WITH (
            id NVARCHAR(50),
            name NVARCHAR(255),
            series NVARCHAR(255),
            printed_total INT '$.printedTotal',
            total INT,
            ptcgo_code NVARCHAR(50) '$.ptcgoCode',
            release_date DATE '$.releaseDate',
            updated_at NVARCHAR(120) '$.updatedAt',
            images NVARCHAR(MAX) AS JSON
        ) as s
        CROSS APPLY OPENJSON(s.images) WITH (symbol_url NVARCHAR(1000) '$.symbol', logo_url NVARCHAR(1000) '$.logo') as images
        )
        INSERT INTO silver.card_set (card_id, set_id, set_card_number, set_name, set_series, set_total_printed, set_total_cards, set_ptcgo_code, set_release_date, set_update_date, set_symbol_image, set_logo_image, row_number)
        SELECT card_id,set_id,set_card_number,set_name,set_series,set_printed_total,set_total_cards,set_ptcgo_code,set_release_date,set_update_date,set_symbol_image,set_logo_image, row_num
        FROM ranked_sets
        order by set_id

        ;
        --==== 10. silver.card_national_pokedex_numbers ====---
        PRINT '--==== LOAD silver.card_national_pokedex_numbers ====---';
        TRUNCATE TABLE silver.card_national_pokedex_numbers;
        INSERT INTO silver.card_national_pokedex_numbers (card_id, national_pokedex_number)
        SELECT c.id as  card_id,
        CAST(npn.value AS INT) as national_pokedex_number
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.national_pokedex_numbers) npn;

        --==== 11. silver.card_legalities ====---
        PRINT '--==== LOAD silver.card_legalities ====---';
        TRUNCATE TABLE silver.card_legalities;
        INSERT INTO silver.card_legalities (card_id, legality)
        SELECT c.id as card_id, 
            CASE WHEN l.standard IS NOT NULL THEN 'Standard+'
                 WHEN l.expanded IS NOT NULL THEN 'Expanded+'
                 WHEN l.unlimited IS NOT NULL THEN 'Unlimited Only' ELSE 'n/a'
            END as legality
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.legalities) WITH (standard NVARCHAR(50), unlimited NVARCHAR(50), expanded NVARCHAR(50)) as l;

        --==== 12. silver.card_images ====---
        PRINT '--==== LOAD silver.card_images ====---';
        TRUNCATE TABLE silver.card_images;
        INSERT INTO silver.card_images (card_id, small_image_url, large_image_url)
        SELECT c.id as card_id,
               im.small as small_image_url,
               im.large as large_image_url
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(c.images) WITH (small NVARCHAR(1000), large NVARCHAR(1000)) as im;

        --==== 13. silver.mkt_tcgplayer ====---
        PRINT '--==== LOAD silver.mkt_tcgplayer ====---';
        TRUNCATE TABLE silver.mkt_tcgplayer;
        INSERT INTO silver.mkt_tcgplayer (card_id, printing_finish_rarity, tcg_updated_at, date_id, market_price, highest_price, lowest_price, direct_buy_price, tcg_url)
        SELECT id as card_id,
               [key] as printing_finish_rarity,
               updated_at as tcg_updated_at,
               CAST(FORMAT(updated_at,'yyyyMMdd') AS INT) as date_id,
               COALESCE(market, (high+low)/2) as market_price,
               high as highest_price,
               low as lowest_price,
               direct_low as direct_buy_price,
               url as tcg_url
        FROM (
            SELECT c.id, tcg.url, tcg.updated_at, p.[key],
                   CAST(JSON_VALUE(p.[value],'$.market') AS float) as market,
                   CAST(JSON_VALUE(p.[value],'$.low') AS float) as low,
                   CAST(JSON_VALUE(p.[value],'$.high') AS float) as high,
                   CAST(JSON_VALUE(p.[value],'$.directLow') AS float) as direct_low
            FROM bronze.cards_json_raw c
            CROSS APPLY OPENJSON(c.tcgplayer) WITH (url NVARCHAR(1000), updated_at DATE '$.updatedAt', prices NVARCHAR(MAX) AS JSON) as tcg
            CROSS APPLY OPENJSON(tcg.prices) as p
        ) t WHERE updated_at > DATEADD(day, -20, GETDATE());

        --==== 14. silver.mkt_cardmarket ====---
        PRINT '--==== LOAD silver.mkt_cardmarket ====---';
        TRUNCATE TABLE silver.mkt_cardmarket;
        INSERT INTO silver.mkt_cardmarket (card_id, cardmarket_updated_at, average_sell_price, low_price, trend_price, german_pro_low, suggested_price, reverse_holo_sell, reverse_holo_low, reverse_holo_trend, low_price_ex_plus, avg_1, avg_7, avg_30, reverse_holo_avg_1, reverse_holo_avg_7, reverse_holo_avg_30, card_url)
        SELECT  c.id as card_id,
                crd.updated_at as cardmarket_updated_at,
                p.*,
                crd.url
        FROM bronze.cards_json_raw c
        CROSS APPLY OPENJSON(cardmarket) WITH (url NVARCHAR(1000), updated_at DATE '$.updatedAt', prices NVARCHAR(MAX) AS JSON) as crd
        CROSS APPLY OPENJSON(prices) WITH (
            average_sell_price FLOAT '$.averageSellPrice', low_price FLOAT '$.lowPrice', trend_price FLOAT '$.trendPrice',
            german_pro_low FLOAT '$.germanProLow', suggested_price FLOAT '$.suggestedPrice', reverse_holo_sell FLOAT '$.reverseHoloSell',
            reverse_holo_low FLOAT '$.reverseHoloLow', reverse_holo_trend FLOAT '$.reverseHoloTrend', low_price_ex_plus FLOAT '$.lowPriceExPlus',
            avg_1 FLOAT '$.avg1', avg_7 FLOAT '$.avg7', avg_30 FLOAT '$.avg30',
            reverse_holo_avg_1 FLOAT '$.reverseHoloAvg1', reverse_holo_avg_7 FLOAT '$.reverseHoloAvg7', reverse_holo_avg_30 FLOAT '$.reverseHoloAvg30'
        ) AS p;


        --NOTE: card_main_info does hold a dependency to 'set' table, so ive placed it all the way below (surrogate key, since this main table is being 'upgraded' instead of inserted

                --==== 1. silver.card_main_info ====---
        PRINT '--==== LOAD silver.card_main_info ====---';
        TRUNCATE TABLE silver.card_main_info;
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
        INSERT INTO silver.card_main_info (card_id, card_name, supertype, hp, evolution_tier, artist, set_card_rarity, set_card_number, description)
        SELECT DISTINCT
            c.id as card_id,
            LOWER(c.name) as card_name,
            LOWER(c.supertype) as supertype,
            CAST(c.hp AS INT) as hp,
            e.evolution as evolution_tier,
            LOWER(c.artist) as artist,
            LOWER(COALESCE(c.rarity, 'special')) as set_card_rarity,
            s.set_card_number,
            LOWER(COALESCE(c.flavor_text, 'n/a')) as description
        FROM bronze.cards_json_raw c
        LEFT JOIN Evolutions e ON c.id = e.id
        LEFT JOIN silver.card_set s ON c.id =s.card_id
        

        SET @end_time = GETDATE();
        PRINT '---------------------------------------------------';
        PRINT 'SUCCESS: Silver Layer loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';
        PRINT '---------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT 'Critical Error during Silver Load: ' + ERROR_MESSAGE();
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
    END CATCH
END;