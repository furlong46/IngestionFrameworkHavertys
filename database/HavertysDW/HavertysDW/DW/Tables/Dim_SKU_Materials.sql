CREATE TABLE [DW].[Dim_SKU_Materials] (
    [Materials_sk]      INT           IDENTITY (1, 1) NOT NULL,
    [Material_Code_nk]  INT           NOT NULL,
    [Material_Name]     VARCHAR (100) NOT NULL,
    [HashKey]           CHAR (40)     NULL,
    [SourceSystem_fk]   INT           NOT NULL,
    [ETLBatchID_Insert] INT           NOT NULL,
    [ETLBatchID_Update] INT           NOT NULL,
    CONSTRAINT [PK_Materials_sk] PRIMARY KEY CLUSTERED ([Materials_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);



