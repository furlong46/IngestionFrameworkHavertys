CREATE PROCEDURE dbo.usp_Delete_Fact_DeliveredSales (
@ControlStartDate varchar(50),
@ControlEndDate varchar(50)
)
AS
DELETE FROM Sales.Fact_DeliveredSales WHERE Delivered_Date_fk BETWEEN CONVERT(int, @ControlStartDate) AND CONVERT(int, @ControlEndDate)