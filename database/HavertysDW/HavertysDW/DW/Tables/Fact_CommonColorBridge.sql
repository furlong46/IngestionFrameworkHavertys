CREATE TABLE [DW].[Fact_CommonColorBridge] (
    [Common_Color_Bridge_sk] INT         IDENTITY (1, 1) NOT NULL,
    [Count]                  INT         NULL,
    [SKU_fk]                 INT         NOT NULL,
    [Common_Color_fk]        INT         NOT NULL,
    [SKU_Unformatted_nk]     VARCHAR (9) NOT NULL,
    [Common_Color_Code_nk]   INT         NULL,
    [SourceSystem_fk]        INT         NOT NULL,
    [ETLBatchID_Insert]      INT         NOT NULL,
    [ETLBatchID_Update]      INT         NOT NULL,
    CONSTRAINT [PK_Common_Color_Bridge_sk] PRIMARY KEY CLUSTERED ([Common_Color_Bridge_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_CommonColorBridge_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_CommonColorBridge_Dim_SKU_CommonColor] FOREIGN KEY ([Common_Color_fk]) REFERENCES [DW].[Dim_SKU_CommonColor] ([Common_Color_sk])
);







