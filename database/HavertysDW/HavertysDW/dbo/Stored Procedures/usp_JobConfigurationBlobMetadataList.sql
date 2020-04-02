CREATE    PROCEDURE [dbo].[usp_JobConfigurationBlobMetadataList] (
@MasterProcessNumber int
)
AS

SELECT 
	JM.StartTime,
	BIL.[BlobFileID], 
	BIL.[BlobFolder], 
	BIL.[FileName], 
	BIL.[BlobFileType], 
	BIL.[DataLakeStagingFolder]
FROM
[ETL].[JobMaster] JM
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='Blob' 
	AND JM.JobConfiguration = JCon.JobConfiguration 
	AND JM.Status IN ('Running','ReRunning') 
	AND JM.MasterProcessNumber = @MasterProcessNumber
INNER JOIN [ETL].[BlobFileMetadataIngestionList] BIL ON JCon.TableID = BIL.BlobFileID AND JCon.ExecuteFlag = CONVERT(bit, 1)
WHERE 
NOT EXISTS (SELECT 1 
			FROM [ETL].[JobTableLog] 
			WHERE MasterProcessNumber = JM.MasterProcessNumber 
				AND TableControlType='Blob' 
				AND TableID = JCon.TableID
				AND JobConfiguration = JM.JobConfiguration 
				AND [Status] IN ('Running','ReRunning','Success'))