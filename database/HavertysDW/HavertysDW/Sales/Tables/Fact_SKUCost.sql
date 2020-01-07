CREATE TABLE [Sales].[Fact_SKUCost] (
    [SKU_Cost_sk]       INT            IDENTITY (1, 1) NOT NULL,
    [Cost]              DECIMAL (9, 2) NULL,
    [SKU_fk]            INT            NOT NULL,
    [StartDate_fk]      INT            NOT NULL,
    [EndDate_fk]        INT            NOT NULL,
    [Cost_Type]         VARCHAR (1)    NULL,
    [Terms]             VARCHAR (4)    NULL,
    [CreatingUser]      VARCHAR (40)   NOT NULL,
    [SourceSystem_fk]   INT            NOT NULL,
    [ETLBatchID_Insert] INT            NOT NULL,
    [ETLBatchID_Update] INT            NOT NULL,
    CONSTRAINT [FK_Fact_SKUCost_Dim_Date_End] FOREIGN KEY ([EndDate_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_SKUCost_Dim_Date_Start] FOREIGN KEY ([StartDate_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_SKUCost_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk])
);










GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_SKUCost]
    ON [Sales].[Fact_SKUCost];





