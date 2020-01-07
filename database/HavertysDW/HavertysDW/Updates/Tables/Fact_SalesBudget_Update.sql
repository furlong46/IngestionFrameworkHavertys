CREATE TABLE [Updates].[Fact_SalesBudget_Update] (
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
    [Sales_Budget_sk_Dest]   INT            NULL
)
WITH (DATA_COMPRESSION = PAGE);





