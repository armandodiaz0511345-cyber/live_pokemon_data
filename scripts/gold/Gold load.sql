/*========================================

GOLD Full Load Script

Purpose: Load Gold layer tables.

exec gold.load_dim_cards - loads (with merges) all the dimesions for the cards (SCD)
1) dim_cards (main info)
2) dim_card_sets (card sets) 
3) dim_cards_extra_info (information about the card such as subtype and type of the pokemon)

rest of script - 
loads the historical data ( creates a log every time it doesnt find a row with the date_id, card_key, and printing_finish_rarity of the silver (daily) data).

========================================*/


CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
--vars here
DECLARE @start_time DATETIME
DECLARE @end_time	DATETIME

BEGIN TRY
SET NOCOUNT ON;

SET @start_time = GETDATE();
--==update dim_cards==-- -- check for new cards, update old ones.
PRINT '---------------------------------------------------';
PRINT '--==updating dim_cards==--'
PRINT '---------------------------------------------------';
exec gold.load_dim_cards; -- done every time we load script, for up-to-date card info.

--==load historical pricing data==--
PRINT '---------------------------------------------------';
PRINT '--==loading historical pricing data==--'
PRINT '---------------------------------------------------';

INSERT INTO gold.fact_hst_tcg_data (card_key,set_key,date_id,full_date,printing_finish_rarity,market_price,highest_price,lowest_price)
SELECT
c.card_key, -- note that cards will show multiple time due to holofoils and normals (tcg)
s.set_key,
d.date_id,
d.full_date,
tcg.printing_finish_rarity,-- add printing finish here and in ddl
tcg.market_price,
tcg.highest_price,
tcg.lowest_price
FROM gold.dim_cards c
INNER JOIN silver.mkt_tcgplayer tcg
ON c.card_id = tcg.card_id
LEFT JOIN gold.dim_dates d
ON  tcg.date_id = d.date_id
LEFT JOIN gold.dim_card_sets s
ON c.set_key = s.set_key
WHERE NOT EXISTS (SELECT * 
					FROM gold.fact_hst_tcg_data hst
					WHERE	hst.date_id = d.date_id
					AND		hst.card_key = c.card_key
					AND		hst.printing_finish_rarity = tcg.printing_finish_rarity
					)
ORDER BY c.card_key, d.date_id

SET @end_time = GETDATE();

PRINT '---------------------------------------------------';
PRINT ' Time to Load: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'
PRINT '---------------------------------------------------';

END TRY

BEGIN CATCH
PRINT 'Critical Error during Silver Load: ' + ERROR_MESSAGE();
PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
END CATCH

END
