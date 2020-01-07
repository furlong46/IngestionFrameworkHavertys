CREATE TABLE [Sales].[Fact_SalesBudget] (
    [Sales_Budget_sk]        INT            IDENTITY (1, 1) NOT NULL,
    [Written_Retail_Sales]   DECIMAL (9, 2) NULL,
    [Delivered_Retail_Sales] DECIMAL (9, 2) NULL,
    [Pool]                   DECIMAL (9, 2) NULL,
    [Date_fk]                INT            NOT NULL,
    [Location_fk]            INT            NOT NULL,
    [BudgetDate_nk]          DATE           NOT NULL,
    [Location_Code_nk]       VARCHAR (15)   NOT NULL,
    [SourceSystem_fk]        INT            NOT NULL,
    [ETLBatchID_Insert]      INT            NOT NULL,
    [ETLBatchID_Update]      INT            NOT NULL,
    CONSTRAINT [PK_Sales_Budget_sk] PRIMARY KEY CLUSTERED ([Sales_Budget_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_SalesBudget_Dim_Date] FOREIGN KEY ([Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_SalesBudget_Dim_Location] FOREIGN KEY ([Location_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk])
);









