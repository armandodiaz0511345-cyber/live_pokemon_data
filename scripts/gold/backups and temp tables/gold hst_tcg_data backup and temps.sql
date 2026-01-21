/*
-=============gold hst_tcg_data backup/editing============-

Purpose:
If anything ever goes wrong with the gold.hst_tcg_data,
use this script to make a backup temp file
of the current table, and then adjust what you need to do (within the temp table)
and then re-insert it all.
==========================================================
*/

SELECT
tcg.card_key,
d.date_id,
d.full_date,
tcg.printing_finish_rarity,
tcg.market_price,
tcg.highest_price,
tcg.lowest_price,
tcg.log_created
INTO #temp_historical_save_new 
FROM gold.hst_tcg_data tcg
LEFT JOIN gold.dim_dates d -- add or subtracts any joins you may need.
on tcg.date_id = d.date_id

select * from #temp_historical_save_new

--ONLY EDIT AND RUN THIS WHEN READY TO MAKE PERMANENT CHANGES

--INSERT INTO gold.hst_tcg_data (card_key,date_id,full_date,printing_finish_rarity,market_price,highest_price,lowest_price,log_created)
--SELECT * from #temp_historical_save_new

--(if anything, just run DDL script to wipe gold table **** ONLY DO IF YOU HAVE THE TEMP BACKUP FILLED)



SELECT
        c.[card_key]
      ,c. [set_key]
      ,c. [card_id]
      ,[card_name]
      ,[supertype]
      ,[evolution_tier]
      ,[artist]
      ,[set_card_rarity]
      ,s.[set_card_number]
      ,[legality]
      ,[number_of_attacks]
      ,[number_of_abilities]
      ,[hp]
      ,[load_time_stamp]
INTO #temp_dim_cards
FROM gold.dim_cards c
LEFT JOIN silver.card_set s
ON c.card_id = s.card_id

select * from #temp_dim_cards
ORDER BY set_key, set_card_number


INSERT INTO gold.dim_cards(set_key, card_id, card_name, supertype, evolution_tier, artist, set_card_rarity, set_card_number, legality, number_of_attacks, number_of_abilities,hp,load_time_stamp)
select set_key, card_id, card_name, supertype, evolution_tier, artist, set_card_rarity, set_card_number, legality, number_of_attacks, number_of_abilities,hp,load_time_stamp from #temp_dim_cards
ORDER BY card_key



SELECT * from gold.dim_cards