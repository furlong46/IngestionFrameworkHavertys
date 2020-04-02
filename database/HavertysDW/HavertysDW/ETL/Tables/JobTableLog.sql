CREATE TABLE [ETL].[JobTableLog] (
    [JobTableLogID]       INT           IDENTITY (1, 1) NOT NULL,
    [MasterProcessNumber] INT           NOT NULL,
    [TableControlType]    VARCHAR (10)  NOT NULL,
    [TableID]             INT           NOT NULL,
    [SchemaTable]         VARCHAR (50)  NOT NULL,
    [JobConfiguration]    VARCHAR (50)  NOT NULL,
    [Status]              VARCHAR (50)  NOT NULL,
    [StartTime]           DATETIME      CONSTRAINT [DF_JobTableLog_StartTime] DEFAULT (getdate()) NOT NULL,
    [ReStartTime]         DATETIME      NULL,
    [EndTime]             DATETIME      NULL,
    [RowsCopied]          INT           NULL,
    [SqlStatementUsed]    VARCHAR (MAX) NULL,
    CONSTRAINT [PK_JobTableLog] PRIMARY KEY CLUSTERED ([JobTableLogID] ASC),
    CONSTRAINT [CH_JobTableLog_TableControlType] CHECK ([TableControlType]='Azure' OR [TableControlType]='DB2' OR [TableControlType]='DW' OR [TableControlType]='Blob'),
    FOREIGN KEY ([JobConfiguration]) REFERENCES [ETL].[JobConfigurationType] ([ConfigurationType]),
    CONSTRAINT [FK_JobTableLog_JobMaster] FOREIGN KEY ([MasterProcessNumber]) REFERENCES [ETL].[JobMaster] ([MasterProcessNumber]),
    CONSTRAINT [AK_OneTablePerJob] UNIQUE NONCLUSTERED ([MasterProcessNumber] ASC, [TableControlType] ASC, [TableID] ASC)
);













