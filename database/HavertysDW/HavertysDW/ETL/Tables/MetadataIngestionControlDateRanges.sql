CREATE TABLE [ETL].[MetadataIngestionControlDateRanges] (
    [TableID]                     INT          NOT NULL,
    [SchemaTable]                 VARCHAR (50) NOT NULL,
    [ControlTable]                VARCHAR (50) NOT NULL,
    [ControlTableStartDateColumn] VARCHAR (50) NOT NULL,
    [ControlTableEndDateColumn]   VARCHAR (50) NOT NULL,
    [ControlProcessNumber]           INT          NOT NULL,
    [ControlStartDate]               DATETIME     NULL,
    [ControlEndDate]                 DATETIME     NULL,
    CONSTRAINT [PK_DB2TableID] PRIMARY KEY CLUSTERED ([TableID] ASC),
    FOREIGN KEY ([TableID]) REFERENCES [ETL].[MetadataIngestionList] ([TableID])
);

