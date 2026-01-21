/*========================================

GOLD DDL SCRIPT

Purpose: create gold tables (no views because these are either:
fact tables,
boiler plate date tables,
or SCD's that i've used MERGE (WHEN MATCHED update, and WHEN NOT MATCHED, add) - in order to maintain any old cards that may be 'discontinued' from the platform.

========================================*/


--This is an AI generated, biolerplate script in order to create a list list of useful DateIDs and enriched date data.
IF OBJECT_ID('gold.dim_dates','U') IS NOT NULL
    DROP TABLE gold.dim_dates;

CREATE TABLE gold.dim_dates (
    date_id           INT PRIMARY KEY, -- Format: 20260116
    full_date         DATE NOT NULL,
    day_number        INT NOT NULL,
    month_number      INT NOT NULL,
    month_name        NVARCHAR(20) NOT NULL,
    quarter          INT NOT NULL,
    year_number       INT NOT NULL,
    day_of_week_name    NVARCHAR(20) NOT NULL,
    is_weekend        BIT NOT NULL,    -- 1 for Sat/Sun, 0 for others
    is_month_end       BIT NOT NULL     -- Useful for monthly price reports
);

IF OBJECT_ID('gold.fact_hst_tcg_data','U') IS NOT NULL
	DROP TABLE gold.fact_hst_tcg_data;
CREATE TABLE gold.fact_hst_tcg_data (
    card_key                INT,
	set_key					INT,
	date_id				    INT, -- tcg_update_at, but formatted to match dim.date
    full_date               DATE,
    printing_finish_rarity  NVARCHAR(50),
	market_price		    FLOAT,
	highest_price		    FLOAT,
	lowest_price		    FLOAT,
	log_created			    DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_PriceHistory PRIMARY KEY (date_id, card_key, printing_finish_rarity)
	)


IF OBJECT_ID('gold.dim_cards','U') IS NOT NULL
	DROP TABLE gold.dim_cards;
CREATE TABLE gold.dim_cards (
   card_key             INT IDENTITY(1,1) PRIMARY KEY,
   set_key              INT,
   card_id              NVARCHAR(50) UNIQUE,
   card_name            NVARCHAR(200),
   supertype            NVARCHAR(50),
   evolution_tier       INT,
   artist               NVARCHAR(255),
   set_card_rarity      NVARCHAR(50),
   set_card_number		INT,
   legality             NVARCHAR(150),
   number_of_attacks    INT,
   number_of_abilities  INT,
   hp                   INT,
   load_time_stamp      DATETIME2 DEFAULT GETDATE()
   )

IF OBJECT_ID('gold.dim_cards_extra_info','U') IS NOT NULL
	DROP TABLE gold.dim_cards_extra_info;
CREATE TABLE gold.dim_cards_extra_info (

card_key				INT PRIMARY KEY, -- this is a composite table (this table will rarely be used so i decided not to create a surrogate key)
subtype					NVARCHAR(50),
type					NVARCHAR(50),
resistance_type			NVARCHAR(50),
--resistance_value		NVARCHAR(50), useless info
weakness_type			NVARCHAR(50),
--weakness_value			NVARCHAR(50), useless info
national_pokedex_number	NVARCHAR(50),
small_image_url			NVARCHAR(200),
large_image_url			NVARCHAR(200)
)



IF OBJECT_ID('gold.dim_card_sets','U') IS NOT NULL
	DROP TABLE gold.dim_card_sets;
CREATE TABLE gold.dim_card_sets (
	set_key				INT IDENTITY(1,1) PRIMARY KEY,
	set_id				NVARCHAR(50),
	set_name			NVARCHAR(255),
	set_series			NVARCHAR(255),
	set_total_printed	INT,
	set_total_cards		INT,
	set_release_date	DATE,
	set_update_date		DATETIME,
	load_time_stamp     DATETIME2 DEFAULT GETDATE()
	)
