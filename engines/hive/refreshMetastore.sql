--Populate metastore with initial dataset available in HDFS

CREATE DATABASE IF NOT EXISTS gdelt_db;
USE gdelt_db;

DROP TABLE IF EXISTS gdelt_temporal;
CREATE EXTERNAL TABLE gdelt_temporal
  (  GLOBALEVENTID 		int
   , SQLDATE			int
   , MonthYear			int
   , Year			int
   , FractionDate		double 
   , Actor1Code			string
   , Actor1Name			string
   , Actor1CountryCode		string
   , Actor1KnownGroupCode	string
   , Actor1EthnicCode		string
   , Actor1Religion1Code	string
   , Actor1Religion2Code	string
   , Actor1Type1Code		string
   , Actor1Type2Code		string
   , Actor1Type3Code		string
   , Actor2Code			string
   , Actor2Name			string
   , Actor2CountryCode		string
   , Actor2KnownGroupCode	string
   , Actor2EthnicCode		string
   , Actor2Religion1Code	string
   , Actor2Religion2Code	string
   , Actor2Type1Code		string
   , Actor2Type2Code		string
   , Actor2Type3Code		string
   , IsRootEvent		int
   , EventCode			string
   , EventBaseCode		string
   , EventRootCode		string
   , QuadClass			int
   , GoldsteinScale		double
   , NumMentions		int
   , NumSources			int
   , NumArticles		int
   , AvgTone			double
   , Actor1Geo_Type		int
   , Actor1Geo_FullName		string 
   , Actor1Geo_CountryCode	string
   , Actor1Geo_ADM1Code		string
   , Actor1Geo_Lat		double
   , Actor1Geo_Long		double
   , Actor1Geo_FeatureID	int      
   , Actor2Geo_Type		int
   , Actor2Geo_FullName		string
   , Actor2Geo_CountryCode	string
   , Actor2Geo_ADM1Code		string
   , Actor2Geo_Lat		double
   , Actor2Geo_Long		double
   , Actor2Geo_FeatureID	int
   , ActionGeo_Type		int
   , ActionGeo_FullName		string
   , ActionGeo_CountryCode	string
   , ActionGeo_ADM1Code		string
   , ActionGeo_Lat		double
   , ActionGeo_Long		double
   , ActionGeo_FeatureID	int
   , DATEADDED			int
   , SOURCEURL			string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE LOCATION '/refresh'
;

INSERT INTO TABLE gdelt_table
SELECT * FROM gdelt_temporal
;
