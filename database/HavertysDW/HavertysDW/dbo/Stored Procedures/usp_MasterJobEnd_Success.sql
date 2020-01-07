


--dbo.usp_MasterJobEnd_Success 243
CREATE   PROCEDURE [dbo].[usp_MasterJobEnd_Success] (
@MasterProcessNumber int
)
AS

DECLARE @Status_Success varchar(50) = 'Success', @Status_Failure varchar(50) = 'Failure', @FailureCount int = 0

SET @FailureCount = ISNULL( (SELECT COUNT(1) FROM [ETL].[JobTableLog] WHERE MasterProcessNumber = @MasterProcessNumber AND Status = 'Failure'), 0)

IF @FailureCount > 0
BEGIN
UPDATE [ETL].[JobMaster] SET [Status] = @Status_Failure, EndTime = GETDATE() WHERE MasterProcessNumber = @MasterProcessNumber 
SELECT 'Stop-' + @Status_Failure AS LogStatus
END

ELSE
BEGIN
UPDATE [ETL].[JobMaster] SET [Status] = @Status_Success, EndTime = GETDATE() WHERE MasterProcessNumber = @MasterProcessNumber 

SELECT 'Stop-' + @Status_Success AS LogStatus

END
