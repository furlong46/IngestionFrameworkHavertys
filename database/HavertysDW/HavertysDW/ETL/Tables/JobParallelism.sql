CREATE TABLE [ETL].[JobParallelism] (
    [JobConfiguration] VARCHAR (50) NOT NULL,
    [JobParallelism]   INT          NOT NULL,
    CONSTRAINT [PK_JobParallelism] PRIMARY KEY CLUSTERED ([JobConfiguration] ASC)
);

