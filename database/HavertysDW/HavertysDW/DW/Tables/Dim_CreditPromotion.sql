CREATE TABLE [DW].[Dim_CreditPromotion] (
    [Credit_Promotion_sk]          INT            IDENTITY (1, 1) NOT NULL,
    [Credit_Promotion_Code_nk]     VARCHAR (4)    NOT NULL,
    [Credit_Promotion_Description] VARCHAR (256)  NOT NULL,
    [Minimum_Sale_Amount]          DECIMAL (8, 2) NOT NULL,
    [Down_Payment_Percentage]      INT            NOT NULL,
    [Plan_Name]                    VARCHAR (100)  NOT NULL,
    [Plan_Description]             VARCHAR (1100) NOT NULL,
    [Duration]                     INT            NOT NULL,
    [Promo_APR]                    DECIMAL (4, 2) NOT NULL,
    [Purchase_APR]                 DECIMAL (4, 2) NOT NULL,
    [HashKey]                      CHAR (40)      NULL,
    [SourceSystem_fk]              INT            NOT NULL,
    [ETLBatchID_Insert]            INT            NOT NULL,
    [ETLBatchID_Update]            INT            NOT NULL,
    CONSTRAINT [PK_Credit_Promotion_sk] PRIMARY KEY CLUSTERED ([Credit_Promotion_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);








GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Credit_Promotion_Code_nk]
    ON [DW].[Dim_CreditPromotion]([Credit_Promotion_Code_nk] ASC) WITH (DATA_COMPRESSION = PAGE);



