/*
======================================

This is an AI generated, biolerplate script in order to populate a list of DATE IDs.


HOW IT WORKS:
-- It utilizes a Recursive CTE w/'DATEADD' function in order to create a list of dates from a specified range.
-- It then takes that date information and enrichens it.


USAGE IN THIS PROJECT:-- This data will be the key to hold this database stores historical data (for fact sheet) - mapping card prices to dates.

======================================
*/

-- Define your date range here
DECLARE @StartDate DATE = '2000-01-01';
DECLARE @EndDate   DATE = '2050-12-31';

TRUNCATE TABLE gold.dim_date;

WITH DateCTE AS (
    SELECT @StartDate as DateValue
    UNION ALL
    SELECT DATEADD(day, 1, DateValue)
    FROM DateCTE
    WHERE DateValue < @EndDate
)
INSERT INTO gold.dim_date (
    date_id, full_date, day_number, month_number, month_name, 
    quarter, year_number, day_of_week_name, is_weekend, is_month_end
)
SELECT 
    CAST(CONVERT(VARCHAR(8), DateValue, 112) AS INT) as DateID,
    DateValue,
    DAY(DateValue),
    MONTH(DateValue),
    DATENAME(month, DateValue),
    DATEPART(quarter, DateValue),
    YEAR(DateValue),
    DATENAME(weekday, DateValue),
    CASE WHEN DATEPART(weekday, DateValue) IN (1, 7) THEN 1 ELSE 0 END, -- Sun=1, Sat=7
    CASE WHEN DATEADD(day, 1, DateValue) = DATEADD(month, DATEDIFF(month, 0, DateValue) + 1, 0) 
         THEN 1 ELSE 0 END
FROM DateCTE
OPTION (MAXRECURSION 32767);
