



--dbo.usp_MasterJobStart 'NightlyLoad'
--dbo.usp_MasterJobStart 'NightlyLoad'
CREATE   PROCEDURE [dbo].[usp_MasterJobStart] (
@JobConfiguration varchar(50)
)
AS

DECLARE @IdentityValue int, @MasterProcessNumber int, @Parallelism int;

SELECT @Parallelism = JobParallelism FROM [ETL].[JobParallelism] WHERE JobConfiguration = @JobConfiguration;

IF EXISTS (SELECT 1 FROM [ETL].[JobMaster] WHERE [Status] IN ('Running','ReRunning'))
BEGIN
SELECT 0 AS MasterProcessNumber
RETURN;
END

--If the MasterProcessNumber already exists, the previous job failed.  Update the status and restarttime.
IF EXISTS (SELECT 1 FROM [ETL].[JobMaster] WHERE [Status]='Failure' AND JobConfiguration = @JobConfiguration)
BEGIN

SELECT @MasterProcessNumber = MasterProcessNumber FROM [ETL].[JobMaster] WHERE [Status]='Failure' AND JobConfiguration = @JobConfiguration

UPDATE [ETL].[JobMaster] SET ReStartTime = GETDATE(), [Status] = 'ReRunning' WHERE [Status]='Failure' AND JobConfiguration = @JobConfiguration
END

--If the MasterProcessNumber is not null, we got the number from DB2 and need to start the process with the seeded number. 
ELSE IF @MasterProcessNumber IS NOT NULL
BEGIN

SET IDENTITY_INSERT [ETL].[JobMaster] ON 
INSERT INTO [ETL].[JobMaster]([MasterProcessNumber], [JobConfiguration], [Status], [StartTime], [ReStartTime], [EndTime])
VALUES
(
@MasterProcessNumber,
@JobConfiguration,
'Running',
GETDATE(),
NULL,
NULL
)
SET IDENTITY_INSERT [ETL].[JobMaster] OFF

END

--If the MasterProcessNumber is null, we're kicking off the job from Azure and we need to generate our own number.  
ELSE 
BEGIN

INSERT INTO [ETL].[JobMaster]([JobConfiguration], [Status], [StartTime], [ReStartTime], [EndTime])
VALUES
(
@JobConfiguration,
'Running',
GETDATE(),
NULL,
NULL
)

SET @IdentityValue = @@IDENTITY

END

SELECT ISNULL(@MasterProcessNumber, @IdentityValue) AS MasterProcessNumber, @Parallelism AS ForEachBatchSize