CREATE TABLE [DW].[Dim_SKU_CommonFinish] (
    [Common_Finish_sk]      INT           IDENTITY (1, 1) NOT NULL,
    [Common_Finish_Code_nk] INT           NOT NULL,
    [Common_Finish_Name]    VARCHAR (100) NOT NULL,
    [HashKey]               CHAR (40)     NULL,
    [SourceSystem_fk]       INT           NOT NULL,
    [ETLBatchID_Insert]     INT           NOT NULL,
    [ETLBatchID_Update]     INT           NOT NULL,
    CONSTRAINT [PK_Common_Finish_sk] PRIMARY KEY CLUSTERED ([Common_Finish_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);



