/*
======================================================
silver DDL load scripts
purpose: load silver tables (dispersed from bronze layer)

======================================================
*/
--===================
--main table
--===================

IF OBJECT_ID('silver.card_main_info','U') IS NOT NULL
	DROP TABLE silver.card_main_info
CREATE TABLE silver.card_main_info (
	card_id				NVARCHAR(50),
	--set_key				INT, -- this will be relationally loaded ( into the gold layer, NOT through here)
	card_name			NVARCHAR(255),
	supertype			NVARCHAR(50),
	hp					INT,
	evolution_tier		INT,
	artist				NVARCHAR(255),
	set_card_rarity		NVARCHAR(50),
	set_card_number		INT,
	description			NVARCHAR(2000),
	load_date			DATETIME DEFAULT GETDATE()
	)

--==========================
--Array tables
--==========================

--attacks table

IF OBJECT_ID('silver.card_attacks','U') IS NOT NULL
	DROP TABLE silver.card_attacks
CREATE TABLE silver.card_attacks (
	card_id					NVARCHAR(50),
	attack_name				NVARCHAR(255),
	energy_needed			NVARCHAR(50),
	energy_amount_neeed		INT,
	total_energy_cost		INT,
	damage					NVARCHAR(255),
	description				NVARCHAR(2000)
	)


IF OBJECT_ID('silver.card_weaknesses','U') IS NOT NULL
	DROP TABLE silver.card_weaknesses
CREATE TABLE silver.card_weaknesses (

	card_id				NVARCHAR(50),
	weakness_type		NVARCHAR(50),
	weakness_value		NVARCHAR(50)
	)

IF OBJECT_ID('silver.card_abilities','U') IS NOT NULL
	DROP TABLE silver.card_abilities
CREATE TABLE silver.card_abilities (

	card_id				NVARCHAR(50),
	ability_name		NVARCHAR(100),
	ability_description	NVARCHAR(2000)
	)

IF OBJECT_ID('silver.card_resistances','U') IS NOT NULL
	DROP TABLE silver.card_resistances
CREATE TABLE silver.card_resistances (

	card_id				NVARCHAR(50),
	resistance_type		NVARCHAR(50),
	resistance_value	NVARCHAR(50)
	)

IF OBJECT_ID('silver.card_retreat_cost','U') IS NOT NULL
	DROP TABLE silver.card_retreat_cost
CREATE TABLE silver.card_retreat_cost (

	card_id					NVARCHAR(50),
	energy_type_to_retreat	NVARCHAR(50)
	)

IF OBJECT_ID('silver.card_subtypes','U') IS NOT NULL
	DROP TABLE silver.card_subtypes
CREATE TABLE silver.card_subtypes (

	card_id					NVARCHAR(50),
	subtype					NVARCHAR(100)
	)

IF OBJECT_ID('silver.card_types','U') IS NOT NULL
	DROP TABLE silver.card_types
CREATE TABLE silver.card_types (

	card_id					NVARCHAR(50),
	[type]					NVARCHAR(50)
	)

--==================
--object tables
--==================

IF OBJECT_ID('silver.card_set','U') IS NOT NULL
	DROP TABLE silver.card_set
CREATE TABLE silver.card_set (

	card_id				NVARCHAR(50),
	set_id				NVARCHAR(50),
	set_card_number		INT,
	set_name			NVARCHAR(255),
	set_series			NVARCHAR(255),
	set_total_printed	INT,
	set_total_cards		INT,
	set_ptcgo_code		NVARCHAR(50),
	set_release_date	DATE,
	set_update_date		DATETIME,
	set_symbol_image	NVARCHAR(1000),
	set_logo_image		NVARCHAR(1000),
	[row_number]		INT
	)

IF OBJECT_ID('silver.card_national_pokedex_numbers','U') IS NOT NULL
	DROP TABLE silver.card_national_pokedex_numbers
CREATE TABLE silver.card_national_pokedex_numbers (

	card_id					NVARCHAR(50),
	national_pokedex_number	INT
	)

IF OBJECT_ID('silver.card_legalities','U') IS NOT NULL
	DROP TABLE silver.card_legalities
CREATE TABLE silver.card_legalities (

	card_id				NVARCHAR(50),
	legality			NVARCHAR(50)
	)

IF OBJECT_ID('silver.card_images','U') IS NOT NULL
	DROP TABLE silver.card_images
CREATE TABLE silver.card_images (

	card_id				NVARCHAR(50),
	small_image_url		NVARCHAR(1000),
	large_image_url		NVARCHAR(1000)
	)

IF OBJECT_ID('silver.mkt_tcgplayer','U') IS NOT NULL
	DROP TABLE silver.mkt_tcgplayer
CREATE TABLE silver.mkt_tcgplayer (					
	card_id						NVARCHAR(50),
	printing_finish_rarity		NVARCHAR(200),
	tcg_updated_at				DATE,
	date_id						INT,
	market_price				FLOAT,
	highest_price				FLOAT,
	lowest_price				FLOAT,
	direct_buy_price			FLOAT,
	tcg_url						NVARCHAR(1000)
	)

IF OBJECT_ID('silver.mkt_cardmarket','U') IS NOT NULL
	DROP TABLE silver.mkt_cardmarket
CREATE TABLE silver.mkt_cardmarket (
	card_id					NVARCHAR(50),
	cardmarket_updated_at	DATE,
	average_sell_price		FLOAT,
    low_price				FLOAT,
    trend_price				FLOAT,
    german_pro_low			FLOAT,
    suggested_price			FLOAT,
    reverse_holo_sell		FLOAT,
    reverse_holo_low		FLOAT,
    reverse_holo_trend		FLOAT,
    low_price_ex_plus		FLOAT,
    avg_1					FLOAT,
    avg_7					FLOAT,
    avg_30					FLOAT,
    reverse_holo_avg_1		FLOAT,
    reverse_holo_avg_7		FLOAT,
    reverse_holo_avg_30		FLOAT,
	card_url				NVARCHAR(1000)
	)


IF OBJECT_ID('silver.card_extras_flattened','U') IS NOT NULL
	DROP TABLE silver.card_extras_flattened
CREATE TABLE silver.card_extras_flattened (
	card_id							NVARCHAR(50),
	subtype_list					NVARCHAR(200),
	type_list						NVARCHAR(200),
	weaknesses_list					NVARCHAR(200),
	resistances_list				NVARCHAR(200),
	pokedex_list					NVARCHAR(200)
	)