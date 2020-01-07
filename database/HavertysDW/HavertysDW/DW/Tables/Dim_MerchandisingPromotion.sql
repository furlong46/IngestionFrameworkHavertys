CREATE TABLE [DW].[Dim_MerchandisingPromotion] (
    [Merchandising_Promotion_sk]          INT          IDENTITY (1, 1) NOT NULL,
    [Merchandising_Promotion_Key_nk]      INT          NOT NULL,
    [Merchandising_Promotion_Code_nk]     VARCHAR (10) NOT NULL,
    [Merchandising_Promotion_Description] VARCHAR (50) NOT NULL,
    [Type_Code]                           VARCHAR (1)  NOT NULL,
    [Type]                                VARCHAR (50) NOT NULL,
    [Start_Date]                          DATE         NOT NULL,
    [End_Date]                            DATE         NOT NULL,
    [Segment_Code]                        INT          NOT NULL,
    [Segment]                             VARCHAR (50) NOT NULL,
    [Available_on_Internet]               VARCHAR (15) NOT NULL,
    [HashKey]                             CHAR (40)    NULL,
    [SourceSystem_fk]                     INT          NOT NULL,
    [ETLBatchID_Insert]                   INT          NOT NULL,
    [ETLBatchID_Update]                   INT          NOT NULL,
    CONSTRAINT [PK_Merchandising_Promotion_sk] PRIMARY KEY CLUSTERED ([Merchandising_Promotion_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);








GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Merchandising_Promotion_Key_nk_Merchandising_Promotion_Code_nk]
    ON [DW].[Dim_MerchandisingPromotion]([Merchandising_Promotion_Key_nk] ASC, [Merchandising_Promotion_Code_nk] ASC) WITH (DATA_COMPRESSION = PAGE);



