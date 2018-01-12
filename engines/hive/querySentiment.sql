USE gdelt_db;

SELECT Actor1CountryCode, avg(AvgTone)
FROM gdelt_table
group by Actor1CountryCode
;
