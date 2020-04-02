
CREATE   PROCEDURE [dbo].[usp_JobTableLogIngestionUpdate] (
@JobTableLogID int,
@RowsCopied int,
@SqlStatementUsed varchar(MAX)
)
AS

UPDATE [ETL].[JobTableLog] SET [RowsCopied] = @RowsCopied, [SqlStatementUsed] = @SqlStatementUsed WHERE JobTableLogID = @JobTableLogID