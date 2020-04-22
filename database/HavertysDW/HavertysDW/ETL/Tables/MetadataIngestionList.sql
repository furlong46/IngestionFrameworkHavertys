CREATE TABLE [ETL].[MetadataIngestionList] (
    [TableID]               INT            IDENTITY (1, 1) NOT NULL,
    [SchemaTable]        VARCHAR (50)   NOT NULL,
    [DataLakeSchemaTable]   VARCHAR (50)   NOT NULL,
    [PKColumnList]          VARCHAR (300)  NOT NULL,
    [FilterColumn]          VARCHAR (50)   NULL,
    [FilterColumnDataType]  VARCHAR (25)   NULL,
    [FilterColumnValue]     VARCHAR (50)   NULL,
    [SelectQuery]           VARCHAR (5000) NOT NULL,
    [DataLakeStagingFolder] VARCHAR (1000) NOT NULL,
    [ServerName]            VARCHAR (25)   NOT NULL,
    [SqlOverrideQuery]      VARCHAR (MAX)  NULL,
    [LoadCurated]           BIT            NOT NULL,
    [Disabled]              BIT            NOT NULL,
    CONSTRAINT [PK_MetadataIngestionList] PRIMARY KEY CLUSTERED ([TableID] ASC)
);

