CREATE VIEW dbo.vw_Current_AzureSQLDB_ServiceTier_ComputeSize 
AS
SELECT DATABASEPROPERTYEX(db_name(),'edition') AS ServiceTier , DATABASEPROPERTYEX(db_name(),'serviceobjective') AS ComputeSize