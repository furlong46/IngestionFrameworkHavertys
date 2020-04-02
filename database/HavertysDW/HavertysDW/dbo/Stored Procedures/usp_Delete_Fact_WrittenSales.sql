CREATE PROCEDURE dbo.usp_Delete_Fact_WrittenSales (
@ControlStartDate varchar(50),
@ControlEndDate varchar(50)
)
AS
DELETE FROM Sales.Fact_WrittenSales WHERE Written_Date_fk BETWEEN CONVERT(int, @ControlStartDate) AND CONVERT(int, @ControlEndDate)