CREATE FUNCTION [dbo].[udf_GenerateSQL]
(@SchemaTable VARCHAR(50),
@FilterColumn VARCHAR(50),
@FilterColumnDataType VARCHAR(50),
@FilterColumnValue VARCHAR(50),
@SelectQuery VARCHAR(5000),
@SqlOverrideQuery VARCHAR(MAX),
@StartDate VARCHAR(50),
@EndDate VARCHAR(50)
)
--Takes in parameters from the AzureMetadataIngestionList to build a query used to select rows from the given table
RETURNS VARCHAR(MAX)
AS

BEGIN

--Checking for NULLs in parameters and defaulting to empty strings

SET @SchemaTable = ISNULL(@SchemaTable, '')
SET @FilterColumn = ISNULL(@FilterColumn, '')
SET @FilterColumnDatatype = ISNULL(@FilterColumnDatatype, '')
SET @FilterColumnValue = ISNULL(@FilterColumnValue, '')
SET @SelectQuery = ISNULL(@SelectQuery, '')
SET @SqlOverrideQuery = ISNULL(@SqlOverrideQuery, '')
SET @StartDate = ISNULL(@StartDate, '')
SET @EndDate = ISNULL(@EndDate, '')


	-- Used in order to set dates back a certain number of days
	DECLARE @CDDCOffset int = (SELECT ConfigValue FROM ETL.Configurations WHERE COnfigKey='CDCOffsetDays'); 

	--Translate this string column into a datetime if applicable
	DECLARE @StartTime DATETIME = (
		CASE WHEN @FilterColumnDatatype <> '' AND ISDATE(@FilterColumnValue) = 1 THEN CONVERT(DATETIME, @FilterColumnValue)
		ELSE NULL END
	);

	--Set the filter value back by CDDCOffset days if it represents a date value
	DECLARE @FilterValue VARCHAR(50) = (
		CASE WHEN ISDATE(@FilterColumnValue) = 1 THEN
			CASE WHEN @FilterColumnDatatype ='TIMESTAMP' THEN CONVERT(varchar(50), DATEADD(day, -1 * @CDDCOffset, @StartTime), 121)
				 WHEN @FilterColumnDatatype = 'INT'		 THEN CONVERT(varchar(50), DATEADD(day, -1 * @CDDCOffset, @StartTime), 112) 
				 WHEN @FilterColumnDatatype = 'DATE'	 THEN CONVERT(varchar(10), DATEADD(day, -1 * @CDDCOffset, @StartTime), 120)
				 ELSE '' END
		ELSE
			CASE WHEN @FilterColumnDatatype = 'INT' THEN @FilterColumnValue
			ELSE '' END
		END
	);

	-- Format the start and end dates based on their type, if they are given
	-- Surround with quotes if needed
	SET @StartDate = (
		CASE WHEN @StartDate = '' THEN ''
		WHEN @EndDate = '' THEN ''
		WHEN @FilterColumnDatatype = 'DATE'	 THEN CONCAT('''', SUBSTRING(@StartDate, 1, 10), '''')
		WHEN @FilterColumnDatatype = 'INT'	 THEN REPLACE(SUBSTRING(@StartDate, 1, 10), '-', '')
		ELSE CONCAT('''', @StartDate, '''') END
	);

	SET @EndDate = (
		CASE WHEN @StartDate = '' THEN ''
		WHEN @EndDate = '' THEN ''
		WHEN @FilterColumnDatatype = 'DATE'	 THEN CONCAT('''', SUBSTRING(@EndDate, 1, 10), '''')
		WHEN @FilterColumnDatatype = 'INT'	 THEN REPLACE(SUBSTRING(@EndDate, 1, 10), '-', '')
		ELSE CONCAT('''', @EndDate, '''') END
	);

	-- If no override query is given, use where clause if filter value or date range is given
	RETURN 
	CASE WHEN COALESCE(@SqlOverrideQuery, '') = '' THEN CONCAT (
		@SelectQuery, ' from ', @SchemaTable, 
		CASE WHEN @StartDate <> '' AND @EndDate <> '' THEN CONCAT(' where ', @FilterColumn, ' between ', @StartDate, ' and ', @EndDate)
		WHEN @FilterColumnDatatype IN ('DATE', 'TIMESTAMP') AND @FilterValue <> ''		THEN CONCAT(' where ', @FilterColumn, ' >= ', '''', @FilterValue, '''')
		WHEN UPPER(COALESCE(@FilterColumnDatatype, '')) = 'INT' AND @FilterValue <> '' THEN CONCAT(' where ', @FilterColumn, ' >= ',  @FilterValue)
		ELSE '' END
	)

	-- If an override query is given, use it. If it includes FILTERCOLUMN and FILTERVALUE, replace these keywords with their given values from the table
	ELSE REPLACE (
		REPLACE(@SqlOverrideQuery, 'FILTERCOLUMN', COALESCE(@FilterColumn, '')),
		'FILTERVALUE',
		CASE WHEN @FilterColumnDatatype IN ('DATE', 'STRING', 'TIMESTAMP') THEN CONCAT('''', @FilterValue, '''')
		ELSE @FilterValue END
	) END
END