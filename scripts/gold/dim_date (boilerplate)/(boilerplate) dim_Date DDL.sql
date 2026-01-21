
/*
======================================

This is an AI generated, biolerplate script in order to create a list list of useful DateIDs and enriched date data.



USAGE IN THIS PROJECT: Creates boilerplate 'date_id' table. 

======================================
*/


IF OBJECT_ID('gold.Dim_Date','U') IS NOT NULL
    DROP TABLE gold.Dim_Date;

CREATE TABLE gold.dim_date (
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