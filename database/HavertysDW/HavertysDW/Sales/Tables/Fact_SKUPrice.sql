CREATE TABLE [Sales].[Fact_SKUPrice] (
    [SKU_Price_sk]      INT            IDENTITY (1, 1) NOT NULL,
    [Price]             DECIMAL (7, 2) NULL,
    [SKU_fk]            INT            NOT NULL,
    [Market_Code]       INT            NOT NULL,
    [Date_fk]           INT            NOT NULL,
    [Price_Type]        VARCHAR (25)   NULL,
    [Short_Description] VARCHAR (15)   NULL,
    [Description]       VARCHAR (30)   NULL,
    [SourceSystem_fk]   INT            NOT NULL,
    [ETLBatchID_Insert] INT            NOT NULL,
    [ETLBatchID_Update] INT            NOT NULL,
    CONSTRAINT [FK_Fact_SKUPrice_Dim_Date] FOREIGN KEY ([Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_SKUPrice_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk])
);






GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_SKUPrice]
    ON [Sales].[Fact_SKUPrice];

