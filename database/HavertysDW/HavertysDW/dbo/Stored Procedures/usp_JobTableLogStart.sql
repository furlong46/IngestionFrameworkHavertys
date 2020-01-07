CREATE      PROCEDURE [dbo].[usp_JobTableLogStart] (
@MasterProcessNumber int,
@TableControlType varchar(10),
@TableID int,
@SchemaTable varchar(50),
@JobConfiguration varchar(50)
)
AS

DECLARE @Status varchar(50), @JobTableLogID int;

--Get the JobTableLog Status and JobTableLogID
SELECT @Status = [Status], @JobTableLogID = JobTableLogID
 FROM [ETL].[JobTableLog] 
 WHERE MasterProcessNumber = @MasterProcessNumber
	AND TableControlType = @TableControlType 
	AND TableID = @TableID
	AND JobConfiguration = @JobConfiguration

--If the job-table is being restarted or already succeeded, then just return
IF @Status IN ('Running','ReRunning','Success')

BEGIN

SELECT 'Stop-RunningOrSuccess' AS LogStatus, @JobTableLogID AS JobTableLogID
RETURN;

END

--If the job-table previously failed, mark it as rerunning
ELSE IF @Status = 'Failure'
BEGIN

UPDATE [ETL].[JobTableLog] SET [Status] = 'ReRunning', ReStartTime = GETDATE() WHERE JobTableLogID = @JobTableLogID

SELECT 'Continue-ReRunning' AS LogStatus, @JobTableLogID AS JobTableLogID
RETURN;

END

--If the job-table hasn't run yet, log a new record
ELSE
BEGIN

INSERT INTO [ETL].[JobTableLog]
VALUES (
@MasterProcessNumber,
@TableControlType,
@TableID,
@SchemaTable,
@JobConfiguration,
'Running',
GETDATE(),
NULL,
NULL
)

SELECT 'Continue-Running' AS LogStatus, @@IDENTITY AS JobTableLogID
RETURN;

END
