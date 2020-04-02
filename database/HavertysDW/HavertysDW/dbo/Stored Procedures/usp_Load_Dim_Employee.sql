CREATE PROCEDURE [dbo].[usp_Load_Dim_Employee] 
AS

UPDATE Dest
SET
Dest.sales_id = Staging.sales_id,
Dest.wms_id = Staging.wms_id,
Dest.driver_id = Staging.driver_id,
Dest.designer_id = Staging.designer_id,
Dest.first_name = Staging.first_name,
Dest.last_name = Staging.last_name,
Dest.full_name = Staging.full_name,
Dest.division_id = Staging.division_id,
Dest.market_id = Staging.market_id,
Dest.branch_id = Staging.branch_id,
Dest.hashkey  = Staging.hashkey,
Dest.ETLBatchID_Update  = Staging.ETLBatchID_Update
FROM
dw.[Dim_Employee] Dest
INNER JOIN [Updates].[Dim_Employee_Update] Staging ON Dest.Employee_id_nk = Staging.Employee_id_nk_Dest