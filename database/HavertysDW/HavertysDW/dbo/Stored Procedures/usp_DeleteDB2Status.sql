-- =============================================
-- Author:      Emma Fick
-- Create Date: 9/25/2019
-- Description: Delete single row from DB2 stored procedure status table before executing REST copy activity in pipeline
-- =============================================
CREATE PROCEDURE [dbo].[usp_DeleteDB2Status]

AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Insert statements for procedure here
    DELETE FROM [ETL].[DB2StoredProcedureStatus];
END