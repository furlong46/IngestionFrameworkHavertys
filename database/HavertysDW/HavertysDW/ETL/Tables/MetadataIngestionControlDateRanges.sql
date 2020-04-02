CREATE TABLE [ETL].[MetadataIngestionControlDateRanges] (
    [DB2TableID]                     INT          NOT NULL,
    [DB2SchemaTable]                 VARCHAR (50) NOT NULL,
    [DB2ControlTable]                VARCHAR (50) NOT NULL,
    [DB2ControlTableStartDateColumn] VARCHAR (50) NOT NULL,
    [DB2ControlTableEndDateColumn]   VARCHAR (50) NOT NULL,
    [ControlProcessNumber]           INT          NOT NULL,
    [ControlStartDate]               DATETIME     NULL,
    [ControlEndDate]                 DATETIME     NULL,
    CONSTRAINT [PK_DB2TableID] PRIMARY KEY CLUSTERED ([DB2TableID] ASC),
    FOREIGN KEY ([DB2TableID]) REFERENCES [ETL].[MetadataIngestionList] ([TableID])
);

