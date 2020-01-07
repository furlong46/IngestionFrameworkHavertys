CREATE TABLE [ETL].[DWLoadList] (
    [TableID]     INT          IDENTITY (1, 1) NOT NULL,
    [SchemaTable] VARCHAR (50) NOT NULL,
    [DWType]      VARCHAR (10) NULL,
    [Disabled]    BIT          CONSTRAINT [DF_DWLoadList_Disabled] DEFAULT ((0)) NULL,
    CONSTRAINT [PK_1] PRIMARY KEY CLUSTERED ([TableID] ASC)
);


GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_DWLoadList]
    ON [ETL].[DWLoadList]([SchemaTable] ASC);

