CREATE TABLE [DW].[Fact_MaterialsBridge] (
    [Materials_Bridge_sk] INT           IDENTITY (1, 1) NOT NULL,
    [Count]               INT           NOT NULL,
    [SKU_fk]              INT           NOT NULL,
    [Materials_fk]        INT           NOT NULL,
    [SKU_Unformatted_nk]  VARCHAR (9)   NOT NULL,
    [Material_Name]       VARCHAR (100) NOT NULL,
    [SourceSystem_fk]     INT           NOT NULL,
    [ETLBatchID_Insert]   INT           NOT NULL,
    [ETLBatchID_Update]   INT           NOT NULL,
    CONSTRAINT [PK_Materials_Bridge_sk] PRIMARY KEY CLUSTERED ([Materials_Bridge_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_MaterialsBridge_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_MaterialsBridge_Dim_SKU_Materials] FOREIGN KEY ([Materials_fk]) REFERENCES [DW].[Dim_SKU_Materials] ([Materials_sk])
);










GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Materials_Bridge]
    ON [DW].[Fact_MaterialsBridge]([SKU_fk] ASC, [Materials_fk] ASC) WITH (DATA_COMPRESSION = PAGE);

