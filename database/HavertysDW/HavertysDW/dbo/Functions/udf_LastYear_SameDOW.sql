


CREATE FUNCTION [dbo].[udf_LastYear_SameDOW]
(@PI_dDATE  DATETIME    
)
RETURNS DATETIME
AS
--DATEADD(week, -52, @PI_dDATE)  this gives the same result
BEGIN
      IF datepart(dw,@PI_ddate) > datepart(dw,dateadd(yy,-1,@PI_ddate))
            RETURN dateadd(dd, datepart(dw,@PI_ddate) - datepart(dw,dateadd(yy,-1,@PI_ddate)) ,dateadd(yy,-1,@PI_ddate))

 

      RETURN dateadd(dd, datepart(dw,@PI_ddate) + (7-datepart(dw,dateadd(yy,-1,@PI_ddate))),dateadd(yy,-1,@PI_ddate))
END