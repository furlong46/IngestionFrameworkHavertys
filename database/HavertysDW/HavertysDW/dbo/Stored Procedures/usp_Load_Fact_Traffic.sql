
CREATE PROCEDURE [dbo].[usp_Load_Fact_Traffic] 
AS
UPDATE Dest
SET
Dest.Staff_on_Floor = Staging.Staff_on_Floor, 
Dest.Ups = Staging.Ups, 
Dest.Call_Customer_Ups = Staging.Call_Customer_Ups,
Dest.Raw_Traffic_Count = Staging.Raw_Traffic_Count,
Dest.Traffic_Count = Staging.Traffic_Count,
Dest.ETLBatchID_Update = Staging.ETLBatchID_Update
FROM
[Sales].[Fact_Traffic] Dest
INNER JOIN [Updates].[Fact_Traffic_Update] Staging on Dest.Traffic_sk = Staging.Traffic_sk_Dest