CREATE TABLE [DW].[Dim_SKU_CommonColor] (
    [Common_Color_sk]      INT           IDENTITY (1, 1) NOT NULL,
    [Common_Color_Code_nk] INT           NOT NULL,
    [Common_Color_Name]    VARCHAR (100) NOT NULL,
    [HashKey]              CHAR (40)     NULL,
    [SourceSystem_fk]      INT           NOT NULL,
    [ETLBatchID_Insert]    INT           NOT NULL,
    [ETLBatchID_Update]    INT           NOT NULL,
    CONSTRAINT [PK_Common_Color_sk] PRIMARY KEY CLUSTERED ([Common_Color_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);



