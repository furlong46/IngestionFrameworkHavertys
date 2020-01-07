
--[dbo].[usp_Load_Dim_Date] '1/1/2000', '12/31/2020'
CREATE    PROCEDURE [dbo].[usp_Load_Dim_Date_old]
	@FirstDateInput DATE = '1/1/2000',	
	@LastDateInput  DATE = '12/31/2020'
AS

DECLARE
@FirstDate DATE = DATEADD(DAY, -7, @FirstDateInput),	
@LastDate  DATE = DATEADD(DAY,  7, @LastDateInput);

SET QUOTED_IDENTIFIER OFF
SET DATEFIRST 7 -- ( Sunday ) default
--SELECT @@DATEFIRST

IF (SELECT COUNT(*) FROM [DW].[Dim_Date] WHERE Calendar_date BETWEEN @FirstDate AND @LastDate) > 1
	  RETURN

DECLARE @CurrentDate DATETIME,
		@MonthName varchar(10),
		@YearName char(4),
		@WeekdayName varchar(10),
		@QuarterPart tinyint,
		@QuarterName varchar(10);

IF OBJECT_ID('tempdb..#Dim_Date') IS NOT NULL
    DROP TABLE #Dim_Date;

IF OBJECT_ID('tempdb..#ReportingDate') IS NOT NULL
    DROP TABLE #ReportingDate;

CREATE TABLE #Dim_Date(
	[Date_sk] [int] NOT NULL,
	[Isworkday_Store] [tinyint] NOT NULL,
	[Isworkday_Delivery] [tinyint] NOT NULL,
	[Calendar_date] [date] NOT NULL,
	[Calendar_name_sht] [varchar](10) NOT NULL,
	[Calendar_name_lng] [varchar](20) NOT NULL,
	[Calendar_year_day_number] [smallint] NOT NULL,
	[Calendar_year_day_name] [varchar](25) NOT NULL,
	[Calendar_quarter_day_number] [tinyint] NOT NULL,
	[Calendar_quarter_day_name] [varchar](20) NOT NULL,
	[Calendar_month_day_number] [tinyint] NOT NULL,
	[Calendar_month_day_name] [varchar](20) NOT NULL,
	[Calendar_weekday_number] [tinyint] NOT NULL,
	[Calendar_weekday_code] [char](3) NOT NULL,
	[Calendar_weekday_name] [varchar](30) NOT NULL,
	[Calendar_week_of_month_number] [tinyint] NOT NULL,
	[Calendar_week_of_month_name] [varchar](20) NOT NULL,
	[Calendar_week_of_year_number] [smallint] NOT NULL,
	[Calendar_week_of_year_name] [varchar](20) NOT NULL,
	[Calendar_month_number] [tinyint] NOT NULL,
	[Calendar_month_code] [char](3) NOT NULL,
	[Calendar_month_name] [varchar](10) NOT NULL,
	[Calendar_quarter_number] [tinyint] NOT NULL,
	[Calendar_quarter_code] [char](2) NOT NULL,
	[Calendar_quarter_name] [varchar](10) NOT NULL,
	[Calendar_month_week_year_number] [int] NOT NULL,
	[Calendar_month_week_year_code] [varchar](20) NOT NULL,
	[Calendar_month_year_number] [int] NOT NULL,
	[Calendar_month_year_code] [varchar](8) NOT NULL,
	[Calendar_quarter_year_number] [int] NOT NULL,
	[Calendar_quarter_year_code] [varchar](10) NOT NULL,
	[Calendar_quarter_year_name] [varchar](15) NOT NULL,
	[Calendar_year_number] [int] NOT NULL,
	[Calendar_year_name] [varchar](4) NOT NULL,
	
	Reporting_week_number [tinyint] NULL,
	Reporting_weekending_date date NULL,
	Reporting_weekending_name varchar(20) NULL,
	Reporting_weekday_number tinyint,

	[Reporting_month_number] [tinyint] NULL,
	[Reporting_month_code] [char](3) NULL,
	[Reporting_month_name] [varchar](10) NULL,
	[Reporting_quarter_number] [tinyint] NULL,
	[Reporting_quarter_code] [char](2) NULL,
	[Reporting_quarter_name] [varchar](10) NULL,
	[Reporting_month_year_number] [int] NULL,
	[Reporting_month_year_code] [varchar](8) NULL,
	[Reporting_quarter_year_number] [int] NULL,
	[Reporting_quarter_year_code] [varchar](10) NULL,
	[Reporting_quarter_year_name] [varchar](15) NULL,
	[Reporting_year_number] [int] NULL,
	[Reporting_year_name] [varchar](4) NULL,

	[Holiday_name] [varchar](20) NULL,
	[ETLBatchID_Insert] [int] NULL,
	[ETLBatchID_Update] [int] NULL
);
		
SET @CurrentDate = @FirstDate;

SET NOCOUNT ON
WHILE @CurrentDate <= @LastDate
BEGIN

	SET @MonthName = DATENAME(month, @CurrentDate);
	SET @YearName = DATENAME(year, @CurrentDate);
	SET @WeekdayName = DATENAME(weekday, @CurrentDate);
	SET @QuarterPart = DATEPART(quarter, @CurrentDate);
	SET @QuarterName = DATENAME(quarter, @CurrentDate);

INSERT INTO #Dim_Date
	(
		Date_sk,
		Isworkday_Store,
		Isworkday_Delivery,
		Calendar_date,
		Calendar_name_sht,
		Calendar_name_lng,
		Calendar_year_day_number,
		Calendar_year_day_name,
		Calendar_quarter_day_number,
		Calendar_quarter_day_name,
		Calendar_month_day_number,
		Calendar_month_day_name,
		Calendar_weekday_number,
		Calendar_weekday_code,
		Calendar_weekday_name,
		Calendar_week_of_month_number,
		Calendar_week_of_month_name,
		Calendar_week_of_year_number,
		Calendar_week_of_year_name,
		Calendar_month_number,
		Calendar_month_code,
		Calendar_month_name,
		Calendar_quarter_number,
		Calendar_quarter_code,
		Calendar_quarter_name,
		Calendar_month_week_year_number,
		Calendar_month_week_year_code,
		Calendar_month_year_number,
		Calendar_month_year_code,
		Calendar_quarter_year_number,
		Calendar_quarter_year_code,
		Calendar_quarter_year_name,
		Calendar_year_number,
		Calendar_year_name,

		Reporting_week_number,
		Reporting_weekending_date,
		Reporting_weekending_name,
		Reporting_weekday_number,
		Reporting_month_number,
		Reporting_month_code,
		Reporting_month_name,
		Reporting_quarter_number,
		Reporting_quarter_code,
		Reporting_quarter_name,
		Reporting_month_year_number,
		Reporting_month_year_code,
		Reporting_quarter_year_number,
		Reporting_quarter_year_code,
		Reporting_quarter_year_name,
		Reporting_year_number,
		Reporting_year_name,

		[Holiday_name],

		ETLBatchID_Insert,
		ETLBatchID_Update
	)
	SELECT
		 [Date_sk] = CASE WHEN @FirstDate = @LastDate THEN 0 ELSE  CAST(CONVERT(VARCHAR(8),@CurrentDate,112) AS INT) END --20060121
		,[Isworkday_Store] = 1 --CASE WHEN DATEPART(dw, @CurrentDate) IN (1, 7) THEN 0 ELSE 1 END
		,[Isworkday_Store] = CASE WHEN DATEPART(dw, @CurrentDate) IN (1, 7) THEN 0 ELSE 1 END
		,[Calendar_dt] = @CurrentDate --2006-01-01
		,[Calendar_nm_sht] = CONVERT(VARCHAR(10), @CurrentDate, 101) --1/21/2007
		,[Calendar_nm_lng] = @MonthName + ' ' + DATENAME(dd, @CurrentDate) + ', ' + @YearName --January 21, 2006
		,[Calendar_year_day_number] = DATEPART(dayofyear, @CurrentDate)
		,[Calendar_year_day_name] = 'Day of year ' + CAST(DATEPART(dayofyear, @CurrentDate) AS VARCHAR(3))
		,[Calendar_quarter_day_number] = DATEDIFF(d, DATEADD(qq, DATEDIFF(qq, 0, @CurrentDate), 0), @CurrentDate) + 1
		,[Calendar_quarter_day_name] = 'Day of quarter ' + CAST(DATEDIFF(d, DATEADD(qq, DATEDIFF(qq, 0, @CurrentDate), 0), @CurrentDate) + 1 AS VARCHAR(2))
		,[Calendar_month_day_number] = DATEPART(DAY, @CurrentDate)
		,[Calendar_month_day_name] = 'Day of month ' + CAST(DATEPART(DAY, @CurrentDate) AS VARCHAR(2))
		,[Calendar_weekday_nr] = DATEPART(weekday, @CurrentDate) --2
		,[Calendar_weekday_cd] = LEFT(@WeekdayName, 3) --Mon
		,[Calendar_weekday_nm] = @WeekdayName --Monday
		--,[Calendar_week_nr] = CASE WHEN DATEPART(DAY, @CurrentDate) IN (1,2,3,4,5,6,7) THEN 1
		--						   WHEN DATEPART(DAY, @CurrentDate) IN (8,9,10,11,12,13,14) THEN 2
		--						   WHEN DATEPART(DAY, @CurrentDate) IN (15,16,17,18,19,20,21) THEN 3
		--						   WHEN DATEPART(DAY, @CurrentDate) IN (22,23,24,25,26,27,28) THEN 4
		--						   ELSE 5 END
		--,[Calendar_week_nm] = 'Week ' + CASE WHEN DATEPART(DAY, @CurrentDate) IN (1,2,3,4,5,6,7) THEN '1'
		--									 WHEN DATEPART(DAY, @CurrentDate) IN (8,9,10,11,12,13,14) THEN '2'
		--									 WHEN DATEPART(DAY, @CurrentDate) IN (15,16,17,18,19,20,21) THEN '3'
		--									 WHEN DATEPART(DAY, @CurrentDate) IN (22,23,24,25,26,27,28) THEN '4'
		--									 ELSE '5' END
		,[Calendar_week_of_month_nr] = (DATEPART(week,@CurrentDate)- DATEPART(week,DATEADD(m, DATEDIFF(m, 0, @CurrentDate), 0))) + 1
		,[Calendar_week_of_month_nm] = 'Week of month ' + CAST((DATEPART(week,@CurrentDate)- DATEPART(week,DATEADD(m, DATEDIFF(m, 0, @CurrentDate), 0))) + 1 AS VARCHAR(2))
		,[Calendar_week_of_year_nr] = DATEPART(week,@CurrentDate)
		,[Calendar_week_of_year_nm] = 'Week of year ' + CAST(DATEPART(week,@CurrentDate) AS VARCHAR(2))
		,[Calendar_month_nr] = DATEPART(month, @CurrentDate) --1
		,[Calendar_month_cd] = LEFT(@MonthName, 3) --Jan
		,[Calendar_month_nm] = @MonthName --January
		,[Calendar_quarter_nr] = @QuarterPart --1
		,[Calendar_quarter_cd] = 'Q' + @QuarterName --Q1
		,[Calendar_quarter_nm] = 'Quarter ' + @QuarterName --Quarter 1
		--,[Calendar_month_week_year_nr] = @YearName
		--	+ CASE WHEN DATEPART(mm, @CurrentDate) < 10 THEN '0' ELSE '' END 
		--	+ CAST(DATEPART(mm, @CurrentDate) AS VARCHAR(2)) --2006011
		--	+ CASE WHEN DATEPART(DAY, @CurrentDate) IN (1,2,3,4,5,6,7) THEN '1'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (8,9,10,11,12,13,14) THEN '2'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (15,16,17,18,19,20,21) THEN '3'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (22,23,24,25,26,27,28) THEN '4'
		--		   ELSE '5' END
		--,[Calendar_month_week_year_cd] = LEFT(@MonthName, 3) + ' Wk ' 
		--	+ CASE WHEN DATEPART(DAY, @CurrentDate) IN (1,2,3,4,5,6,7) THEN '1'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (8,9,10,11,12,13,14) THEN '2'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (15,16,17,18,19,20,21) THEN '3'
		--		   WHEN DATEPART(DAY, @CurrentDate) IN (22,23,24,25,26,27,28) THEN '4'
		--		   ELSE '5' END
		--	+ ' ' + @YearName --Jan 06
		,[Calendar_month_week_year_nr] = @YearName
			+ CASE WHEN DATEPART(mm, @CurrentDate) < 10 THEN '0' ELSE '' END 
			+ CAST(DATEPART(mm, @CurrentDate) AS VARCHAR(2)) --2006011
			+ CAST((DATEPART(week,@CurrentDate)- DATEPART(week,DATEADD(m, DATEDIFF(m, 0, @CurrentDate), 0))) + 1 AS VARCHAR(2))
		,[Calendar_month_week_year_cd] = LEFT(@MonthName, 3) + ' Wk ' 
			+ CAST((DATEPART(week,@CurrentDate)- DATEPART(week,DATEADD(m, DATEDIFF(m, 0, @CurrentDate), 0))) + 1 AS VARCHAR(2))
			+ ' ' + @YearName --Jan 06
		,[Calendar_month_year_nr] = @YearName 
			+ CASE WHEN DATEPART(mm, @CurrentDate) < 10 THEN '0' ELSE '' END 
			+ CAST(DATEPART(mm, @CurrentDate) AS VARCHAR(2)) --200601
		,[Calendar_month_year_cd] = LEFT(@MonthName, 3) + ' ' + @YearName --Jan 06
		,[Calendar_quarter_year_nr] = CAST(@YearName + LTRIM(STR(@QuarterPart)) AS INT) --20061
		,[Calendar_quarter_year_cd] = 'Qtr ' + @QuarterName + ' ' + @YearName --Qtr 1 2006
		,[Calendar_quarter_year_nm] = 'Quarter ' + @QuarterName + ', ' + @YearName --Quarter 1, 2006
		,[Calendar_year_nr] = DATEPART(year, @CurrentDate) --2006
		,[Calendar_year_nm] = @YearName --2006

		,Reporting_week_number = NULL
		,Reporting_weekending_date = NULL
		,Reporting_weekending_name = NULL
		,Reporting_weekday_number = NULL
		,Reporting_month_number = NULL
		,Reporting_month_code = NULL
		,Reporting_month_name = NULL
		,Reporting_quarter_number = NULL
		,Reporting_quarter_code = NULL
		,Reporting_quarter_name = NULL
		,Reporting_month_year_number = NULL
		,Reporting_month_year_code = NULL
		,Reporting_quarter_year_number = NULL
		,Reporting_quarter_year_code = NULL
		,Reporting_quarter_year_name = NULL
		,Reporting_year_number = NULL
		,Reporting_year_name = NULL

		,[Holiday_name] = ''

		,0 --ETLBatchID_Insert
		,0 --ETLBatchID_Update

	SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate)

END; --WHILE @CurrentDate <= @LastDate'


/* Reporting Hierarchy Section */
SET DATEFIRST 2; -- ( Tuesday )

UPDATE #Dim_Date
SET [Reporting_week_number] = DATEPART(week, Calendar_date);

WITH Reporting AS (
SELECT 
Calendar_date,  
[Calendar_weekday_number],
[Calendar_weekday_code],
[Calendar_weekday_name],
[Calendar_week_of_year_number],
[Reporting_week_number_smooth] = 
	CASE WHEN [Reporting_week_number] = 53 AND RIGHT(Calendar_date, 5) IN 
		('12-31','12-30','12-29','12-28','12-27','12-26','12-25') 
		AND FoYWN.First_of_Year_weekday_number <> 3
		THEN 1 
	ELSE [Reporting_week_number] END,
[Reporting_year_number] = 
	CASE WHEN [Reporting_week_number] = 53 AND RIGHT(Calendar_date, 5) IN 
		('12-31','12-30','12-29','12-28','12-27','12-26','12-25') 
		AND FoYWN.First_of_Year_weekday_number <> 3
		THEN Calendar_year_number + 1 
	ELSE Calendar_year_number END,
[Reporting_year_name] = 
	CONVERT(varchar(4),
	CASE WHEN [Reporting_week_number] = 53 AND RIGHT(Calendar_date, 5) IN 
		('12-31','12-30','12-29','12-28','12-27','12-26','12-25') 
		AND FoYWN.First_of_Year_weekday_number <> 3
		THEN Calendar_year_number + 1 
	ELSE Calendar_year_number END)

FROM #Dim_Date
INNER JOIN (SELECT YEAR(Calendar_date) AS [Year], [Calendar_weekday_number] AS First_of_Year_weekday_number FROM #Dim_Date WHERE RIGHT(Calendar_date, 5) = '01-01') FoYWN 
	ON #Dim_Date.Calendar_year_number + 1 = FoYWN.[Year]
)
,
RWE AS (
SELECT Reporting.*, 
Reporting_weekday_number = ROW_NUMBER() OVER(PARTITION BY Reporting_year_number, Reporting_week_number_smooth ORDER BY Calendar_date),
Reporting_weekending_date = LAST_VALUE(Calendar_date) OVER(PARTITION BY Reporting_year_number, Reporting_week_number_smooth ORDER BY Calendar_date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM Reporting
)


SELECT 
RWE.*, 
Reporting_weekending_name = 'WE ' + RWE.Reporting_year_name + ' ' + Calendar_month_code + ' ' + RIGHT(RWE.Reporting_weekending_date, 2),
Reporting_month_number = #Dim_Date.Calendar_month_number,
Reporting_month_code = #Dim_Date.Calendar_month_code,
Reporting_month_name = #Dim_Date.Calendar_month_name,
Reporting_quarter_number = #Dim_Date.Calendar_quarter_number,
Reporting_quarter_code = #Dim_Date.Calendar_quarter_code,
Reporting_quarter_name = #Dim_Date.Calendar_quarter_name,
Reporting_month_year_number = #Dim_Date.Calendar_month_year_number,
Reporting_month_year_code = #Dim_Date.Calendar_month_year_code,
Reporting_quarter_year_number = #Dim_Date.Calendar_quarter_year_number,
Reporting_quarter_year_code = #Dim_Date.Calendar_quarter_year_code,
Reporting_quarter_year_name = #Dim_Date.Calendar_quarter_year_name
INTO #ReportingDate
FROM RWE
INNER JOIN #Dim_Date ON RWE.Reporting_weekending_date = #Dim_Date.Calendar_date
ORDER BY RWE.Calendar_date;


UPDATE #Dim_Date
SET
#Dim_Date.Reporting_week_number = #ReportingDate.Reporting_week_number_smooth,
#Dim_Date.Reporting_weekending_date = #ReportingDate.Reporting_weekending_date,
#Dim_Date.Reporting_weekending_name = #ReportingDate.Reporting_weekending_name,
#Dim_Date.Reporting_weekday_number = #ReportingDate.Reporting_weekday_number,
#Dim_Date.Reporting_month_number = #ReportingDate.Reporting_month_number,
#Dim_Date.Reporting_month_code = #ReportingDate.Reporting_month_code,
#Dim_Date.Reporting_month_name = #ReportingDate.Reporting_month_name,
#Dim_Date.Reporting_quarter_number = #ReportingDate.Reporting_quarter_number,
#Dim_Date.Reporting_quarter_code = #ReportingDate.Reporting_quarter_code,
#Dim_Date.Reporting_quarter_name = #ReportingDate.Reporting_quarter_name,
#Dim_Date.Reporting_month_year_number = #ReportingDate.Reporting_month_year_number,
#Dim_Date.Reporting_month_year_code = #ReportingDate.Reporting_month_year_code,
#Dim_Date.Reporting_quarter_year_number = #ReportingDate.Reporting_quarter_year_number,
#Dim_Date.Reporting_quarter_year_code = #ReportingDate.Reporting_quarter_year_code,
#Dim_Date.Reporting_quarter_year_name = #ReportingDate.Reporting_quarter_year_name,
#Dim_Date.Reporting_year_number = #ReportingDate.Reporting_year_number,
#Dim_Date.Reporting_year_name = #ReportingDate.Reporting_year_name
FROM
#Dim_Date
INNER JOIN #ReportingDate ON #Dim_Date.Calendar_date = #ReportingDate.Calendar_date;

INSERT INTO DW.Dim_Date
SELECT * FROM #Dim_Date WHERE Calendar_date BETWEEN @FirstDateInput AND @LastDateInput