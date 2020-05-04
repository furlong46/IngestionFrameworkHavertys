


CREATE    PROCEDURE [dbo].[usp_JobConfigurationMetadataList] (
@MasterProcessNumber int
)
AS

SELECT 
	JM.StartTime,
	IL.[TableID], 
	IL.[SchemaTable], 
	IL.[DataLakeSchemaTable],
	IL.[PKColumnList], 
	IL.[FilterColumn], 
	IL.[FilterColumnDataType],
	IL.[FilterColumnValue],
	IL.[SelectQuery], 
	IL.[DataLakeStagingFolder],
	IL.[ServerName],
	IL.[SqlOverrideQuery],
	IL.[CreateRawTable],
	IL.[LoadCurated],
	CDR.[ControlTable],
	CDR.[ControlProcessNumber], 
	CDR.[ControlStartDate],
	CDR.[ControlEndDate],
	CONCAT(
		'select ',
		CASE WHEN CDR.[ControlTable] IS NULL THEN 'cast(null as date) ' ELSE CDR.[ControlTableStartDateColumn] END,
		' as start_date, ',
		CASE WHEN CDR.[ControlTable] IS NULL THEN 'cast(null as date) ' ELSE CDR.[ControlTableEndDateColumn] END,
		' as end_date, ',
		CASE WHEN CDR.[ControlTable] IS NULL THEN '0' ELSE 'next_process_number ' END,
		' as next_process_number from ', 
		CASE WHEN CDR.[ControlTable] IS NULL THEN 'sysibm.sysdummy1'  ELSE CDR.[ControlTable] END
	) 
	as ControlTableSql

FROM
[ETL].[JobMaster] JM
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='Azure' 
	AND JM.JobConfiguration = JCon.JobConfiguration 
	AND JM.Status IN ('Running','ReRunning') 
	AND JM.MasterProcessNumber = @MasterProcessNumber 
INNER JOIN [ETL].[MetadataIngestionList] IL ON JCon.TableID = IL.TableID AND JCon.ExecuteFlag = CONVERT(bit, 1) AND IL.[Disabled] = CONVERT(bit,  0)
LEFT OUTER JOIN [ETL].[MetadataIngestionControlDateRanges] CDR ON IL.TableID = CDR.TableID
WHERE 
NOT EXISTS (SELECT 1 
			FROM [ETL].[JobTableLog] 
			WHERE MasterProcessNumber = JM.MasterProcessNumber 
			 	AND TableControlType='Azure' 
				AND TableID = JCon.TableID
				AND JobConfiguration = JM.JobConfiguration 
				AND [Status] IN ('Running','ReRunning','Success')) -- This table must not have an existing successful or running log for this run 
