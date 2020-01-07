CREATE TABLE [ETL].[DB2MetadataIngestionList] (
    [DB2TableID]            INT            IDENTITY (1, 1) NOT NULL,
    [SchemaTable]           VARCHAR (50)   NOT NULL,
    [PKColumnList]          VARCHAR (300)  NOT NULL,
    [ControlTable]          VARCHAR (50)   NOT NULL,
    [ControlFilterColumn]   VARCHAR (50)   NULL,
    [ControlStartDate]      DATETIME       NULL,
    [ControlEndDate]        DATETIME       NULL,
    [ControlFilterValue]    VARCHAR (50)   NULL,
    [ControlFilterType]     VARCHAR (25)   NULL,
    [ControlProcessNumber]  INT            NOT NULL,
    [SelectQuery]           VARCHAR (5000) NOT NULL,
    [DataLakeStagingFolder] VARCHAR (1000) NOT NULL,
    CONSTRAINT [PK_DB2MetadataIngestionList] PRIMARY KEY CLUSTERED ([DB2TableID] ASC)
);



