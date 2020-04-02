-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS DataLakeStaging

-- COMMAND ----------

DROP TABLE IF EXISTS DataLakeStaging.pcrundata_tbcid;
CREATE TABLE IF NOT EXISTS DataLakeStaging.pcrundata_tbcid (
CIDNUM	decimal(38,18),
CREATED	timestamp,
SOURCE	string,
UPDATED	timestamp,
STATUS	string,
SYNCHED	timestamp,
PREFNAM	decimal(38,18),
PREFPHN	decimal(38,18),
PREFADR	decimal(38,18),
SENDNEWS string,
SENDADV	string,
SENDCAT	string,
PREFEML	decimal(38,18),
SENDPMAIL string,
NOEMAIL	string,
ROOMPLAN string
)
USING PARQUET
LOCATION '/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcid/'
OPTIONS ('compression'='snappy')

-- COMMAND ----------

DROP TABLE IF EXISTS DataLakeStaging.pcrundata_tbcidprf;
CREATE TABLE IF NOT EXISTS DataLakeStaging.pcrundata_tbcidprf (
CIDNUM	decimal(38,18),
SEQNUM	decimal(38,18),
CREATED	timestamp,
UPDATED	timestamp,
STATUS	string,
BILNAMSEQ	decimal(38,18),
BILADRSEQ	decimal(38,18),
DLVNAMSEQ	decimal(38,18),
DLVADRSEQ	decimal(38,18)
)
USING PARQUET
LOCATION '/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcidprf/'
OPTIONS ('compression'='snappy')

-- COMMAND ----------

DROP TABLE IF EXISTS DataLakeStaging.ipcorpdta_tbdwwrt;
CREATE TABLE IF NOT EXISTS DataLakeStaging.ipcorpdta_tbdwwrt (
PROCESS_NUMBER	decimal(38,18)  
,DIVISION	decimal(38,18)
,MARKET	decimal(38,18)
,BRANCH	decimal(38,18)
,SALENUM	decimal(38,18)
,LINE	decimal(38,18)
,TYPE	string
,TRANTYPE	string
,TRANSEQUENCE	decimal(38,18)
,ORGSALENUM	decimal(38,18)
,WRITTENDATE	timestamp
,WRITTENTIME	decimal(38,18)
,ITEM	string
,GROUPSKU	string
,QUANTITY	decimal(38,18)
,GROSSSLS	decimal(38,18)
,GRPSKUDSCT	decimal(38,18)
,RTLREDUCTION	decimal(38,18)
,OVRDISCOUNT	decimal(38,18)
,PROMODISCOUNT	decimal(38,18)
,UNCLASSDSCT	decimal(38,18)
,RETAIL	decimal(38,18)
,TAXAMT1	decimal(38,18)
,TAXAMT2	decimal(38,18)
,CIDNUM	decimal(38,18)
,CIDPRFSEQ	decimal(38,18)
,SALEASC	decimal(38,18)
,SALEEMPID	decimal(38,18)
,COSALEASC	decimal(38,18)
,COSALEEMPID	decimal(38,18)
,DESIGNER	decimal(38,18)
,CODESIGNER	decimal(38,18)
,OVRRSNID	decimal(38,18)
,OVRRSNDESC	string
,OVRRSNCMT	string
,OVRUSRFNAM	string
,OVRUSRLNAM	string
,SALEPROMO	string
,CREDITPROMO	string
,DELIVERYDC	decimal(38,18)
,CHARGETYPE	string
,INTERNETSALE	string
,EXCHANGETYPE	string
,GROUPLINE	decimal(38,18)
,ITEMSINGROUP	decimal(38,18)
,OMNICHANNEL	string
,DELIVERYTYPE	string
,ORIGINATION	string
,BOLPIS	string
)
USING PARQUET
LOCATION '/mnt/datalake/staging/sales/DB203002/ipcorpdta_tbdwwrt/'
OPTIONS ('compression'='snappy')

-- COMMAND ----------

DROP TABLE IF EXISTS DataLakeStaging.ipcorpdta_tbdwbld;
CREATE TABLE IF NOT EXISTS DataLakeStaging.ipcorpdta_tbdwbld (
PROCESS_NUMBER	decimal(38,18)
,DIVISION	decimal(38,18)
,MARKET	decimal(38,18)
,BRANCH	decimal(38,18)
,SALENUM	decimal(38,18)
,LINE	decimal(38,18)
,TYPE	string
,TRANTYPE	string
,TRANSEQUENCE	decimal(38,18)
,ORGSALENUM	decimal(38,18)
,BILLEDDATE	timestamp
,WRITTENDATE	timestamp
,ITEM	string
,GROUPSKU	string
,QUANTITY	decimal(38,18)
,GROSSSLS	decimal(38,18)
,GRPSKUDSCT	decimal(38,18)
,RTLREDUCTION	decimal(38,18)
,OVRDISCOUNT	decimal(38,18)
,PROMODISCOUNT	decimal(38,18)
,UNCLASSDSCT	decimal(38,18)
,UNDLVALLOW	decimal(38,18)
,RETAIL	decimal(38,18)
,TAXAMT1	decimal(38,18)
,TAXAMT2	decimal(38,18)
,CIDNUM	decimal(38,18)
,CIDPRFSEQ	decimal(38,18)
,SALEASC	decimal(38,18)
,SALEEMPID	decimal(38,18)
,COSALEASC	decimal(38,18)
,COSALEEMPID	decimal(38,18)
,DESIGNER	decimal(38,18)
,CODESIGNER	decimal(38,18)
,OVRRSNID	decimal(38,18)
,OVRRSNDESC	string
,OVRRSNCMT	string
,OVRUSRFNAM	string
,OVRUSRLNAM	string
,SALEPROMO	string
,CREDITPROMO	string
,DELIVERYDC	decimal(38,18)
,LANDEDCOST	decimal(38,18)
,CHARGETYPE	string
,INTERNETSALE	string
,EXCHANGETYPE	string
,GROUPLINE	decimal(38,18)
,ITEMSINGROUP	decimal(38,18)
,OMNICHANNEL	string
,DELIVERYTYPE	string
,ORIGINATION	string
,BOLPIS	string
)
USING PARQUET
LOCATION '/mnt/datalake/staging/sales/DB203002/ipcorpdta_tbdwbld/'
OPTIONS ('compression'='snappy')

-- COMMAND ----------

