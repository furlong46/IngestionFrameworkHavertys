
CREATE   PROCEDURE [dbo].[usp_MetadataListUpdate] (
@JobTableLogID int,
@StartDate datetime, -- Extracted_from
@EndDate datetime, -- Extracted_thru
@ProcessNumber int -- Next_process_number
)
AS

DECLARE @StartTime datetime, @TableID int

SELECT @StartTime = StartTime, @TableID = TableID 
FROM [ETL].[JobTableLog] 
WHERE JobTableLogID = @JobTableLogID

-- Updates rows to the correct format
UPDATE [ETL].[MetadataIngestionList] SET FilterColumnValue = convert(varchar(50), @StartTime, 121) 
WHERE TableID = @TableID AND FilterColumn IS NOT NULL AND FilterColumnValue IS NOT NULL And COALESCE(FilterColumnDatatype, '') = 'TIMESTAMP';

UPDATE [ETL].[MetadataIngestionList] SET FilterColumnValue = convert(varchar(50), @StartTime, 112) 
WHERE TableID = @TableID AND FilterColumn IS NOT NULL AND FilterColumnValue IS NOT NULL AND COALESCE(FilterColumnDatatype, '') = 'INT' AND ISDATE(FilterColumnValue) = 1 

UPDATE [ETL].[MetadataIngestionList] SET FilterColumnValue = convert(varchar(10), @StartTime, 120) 
WHERE TableID = @TableID AND FilterColumn IS NOT NULL AND FilterColumnValue IS NOT NULL And COALESCE(FilterColumnDatatype, '') = 'DATE'

DECLARE @Count INT = ISNULL((SELECT COUNT(1) FROM [ETL].[MetadataIngestionControlDateRanges] WHERE TableID = @TableID), 0)

IF @Count > 0
BEGIN
	UPDATE [ETL].[MetadataIngestionControlDateRanges] 
	SET ControlStartDate = @StartDate, ControlEndDate = @EndDate, ControlProcessNumber = @ProcessNumber
	WHERE TableID = @TableID 
END