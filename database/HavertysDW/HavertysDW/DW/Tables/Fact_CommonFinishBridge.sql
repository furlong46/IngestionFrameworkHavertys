CREATE TABLE [DW].[Fact_CommonFinishBridge] (
    [Common_Finish_Bridge_sk] INT IDENTITY (1, 1) NOT NULL,
    [Count]                   INT NULL,
    [SKU_fk]                  INT NOT NULL,
    [Common_Finish_fk]        INT NOT NULL,
    [Common_Finish_Code_nk]   INT NOT NULL,
    [SKU_Unformatted_nk]      INT NOT NULL,
    [SourceSystem_fk]         INT NOT NULL,
    [ETLBatchID_Insert]       INT NOT NULL,
    [ETLBatchID_Update]       INT NOT NULL,
    CONSTRAINT [PK_Common_Finish_Bridge_sk] PRIMARY KEY CLUSTERED ([Common_Finish_Bridge_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_CommonFinishBridge_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_CommonFinishBridge_Dim_SKU_CommonFinish] FOREIGN KEY ([Common_Finish_fk]) REFERENCES [DW].[Dim_SKU_CommonFinish] ([Common_Finish_sk])
);







