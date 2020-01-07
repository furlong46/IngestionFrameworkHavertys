

--dbo.usp_MasterJobStart 1, 'NightlyLoad'
CREATE   PROCEDURE [dbo].[usp_MasterJobEnd_Failure] (
@MasterProcessNumber int
)
AS

DECLARE @Status varchar(50) = 'Failure'

UPDATE [ETL].[JobMaster] SET [Status] = @Status, EndTime = GETDATE() WHERE MasterProcessNumber = @MasterProcessNumber 

SELECT 'Stop-' + @Status AS LogStatus
