


CREATE VIEW [dbo].[Compressed_Tables]
AS
--ALTER TABLE Production.TransactionHistory REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);   
--ALTER INDEX IX_TransactionHistory_ProductID ON Production.TransactionHistory REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);  
SELECT DISTINCT
SERVERPROPERTY('servername') [instance]
,DB_NAME() [database]
,QUOTENAME(OBJECT_SCHEMA_NAME(sp.object_id)) +'.'+QUOTENAME(Object_name(sp.object_id)) AS SchemaTable
,ix.name [index_name]
,sp.data_compression
,sp.data_compression_desc
FROM sys.partitions SP
LEFT OUTER JOIN sys.indexes IX
ON sp.object_id = ix.object_id
and sp.index_id = ix.index_id
WHERE --sp.data_compression <> 0
QUOTENAME(OBJECT_SCHEMA_NAME(sp.object_id)) IN ('[DW]','[Sales]','[Updates]','[Inventory]')