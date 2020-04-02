CREATE PROCEDURE [dbo].[usp_Load_Fact_SalesBudgets] 
AS
UPDATE Dest
SET
Dest.Written_Retail_Sales = Staging.Written_Retail_Sales, 
Dest.Delivered_Retail_Sales = Staging.Delivered_Retail_Sales, 
Dest.ETLBatchID_Update = Staging.ETLBatchID_Update
FROM
[Sales].[Fact_SalesBudget] Dest
INNER JOIN [Updates].[Fact_SalesBudget_Update] Staging on Dest.Sales_Budget_sk = Staging.Sales_Budget_sk_Dest