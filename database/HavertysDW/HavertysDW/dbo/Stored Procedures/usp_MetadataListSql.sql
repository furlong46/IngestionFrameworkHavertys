CREATE    PROCEDURE [dbo].[usp_MetadataListSql] (
@DB2SchemaTable VARCHAR(50),
@FilterColumn VARCHAR(50),
@FilterColumnDataType VARCHAR(50),
@FilterColumnValue VARCHAR(50),
@SelectQuery VARCHAR(5000),
@SqlOverrideQuery VARCHAR(MAX),
@StartDate VARCHAR(50),
@EndDate VARCHAR(50)
)
AS

-- This stored procedure is used to allow ADF to call this scalar function since that can't be done directly
SELECT 
dbo.udf_GenerateSql(@DB2SchemaTable, @FilterColumn, @FilterColumnDataType, @FilterColumnValue, @SelectQuery, @SqlOverrideQuery, @StartDate, @EndDate) AS Query