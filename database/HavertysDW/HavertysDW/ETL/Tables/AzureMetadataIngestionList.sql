CREATE TABLE [ETL].[AzureMetadataIngestionList] (
    [AzureTableID]          INT            IDENTITY (1, 1) NOT NULL,
    [SchemaTable]           VARCHAR (50)   NOT NULL,
    [PKColumnList]          VARCHAR (300)  NOT NULL,
    [FilterColumn]          VARCHAR (50)   NULL,
    [FilterColumnDatatype]  VARCHAR (25)   NULL,
    [FilterColumnValue]     DATETIME       NULL,
    [SelectQuery]           VARCHAR (5000) NOT NULL,
    [DataLakeStagingFolder] VARCHAR (1000) NOT NULL,
    [Disabled]              BIT            CONSTRAINT [DF_AzureMetadataIngestionList_Disabled] DEFAULT ((0)) NULL,
    CONSTRAINT [PK_AzureMetadataIngestionList] PRIMARY KEY CLUSTERED ([AzureTableID] ASC)
);








GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_AzureMetadataIngestionList]
    ON [ETL].[AzureMetadataIngestionList]([SchemaTable] ASC);

