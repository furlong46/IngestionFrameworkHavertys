CREATE PROCEDURE [dbo].[usp_Load_Dim_Time]
AS
/*---------------------------------------------------
	
 Author         : Leo Furlong
 Parameters     : 
 Purpose        : Populate time dimension at the second level. 86,400 records expected. The target table structure is as follows:
				 
CREATE TABLE [DW].[Dim_Time](
	[Time_sk] [int] IDENTITY(1,1) NOT NULL,
	[Time24_code] [time](0) NOT NULL,
	[Time24_name] [varchar](8) NOT NULL,
	[Hour_code] [tinyint] NOT NULL,
	[Hour_name] [varchar](2) NOT NULL,
	[Hour24_code] [tinyint] NOT NULL,
	[Hour24_name] [varchar](2) NOT NULL,
	[Minute_code] [tinyint] NOT NULL,
	[Minute_name] [varchar](2) NOT NULL,
	[Minute24_code] [smallint] NOT NULL,
	[Minute24_name] [varchar](4) NOT NULL,
	[Second_code] [tinyint] NOT NULL,
	[Second_name] [varchar](2) NOT NULL,
	[Second24_code] [int] NOT NULL,
	[Second24_name] [varchar](5) NOT NULL,
	[AmPm] [varchar](2) NOT NULL,
	[StandardTime] [varchar](11) NOT NULL,
	[ETLBatchID_Insert] [int] NOT NULL,
	[ETLBatchID_Update] [int] NOT NULL,
 CONSTRAINT [PK_Dim_Time] PRIMARY KEY CLUSTERED 
(
	[Time_sk] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [Primary]
) ON [Primary]

 ------------------------------------------------------------*/
--Load time data for every second of a day 
DECLARE @Time TIME 
DECLARE @i INT
SET @i=1
SET @Time = '00:00:00'


WHILE @i <= 86400 
BEGIN 
 INSERT INTO DW.Dim_Time
 SELECT @Time AS Time24_Code
        ,CAST(@Time as varchar(8)) AS Time24_name		
        , CASE 
		    WHEN DATEPART(HOUR,@Time) = 0 THEN 12
            WHEN DATEPART(HOUR,@Time) > 12 THEN DATEPART(HOUR,@Time) - 12 
            ELSE DATEPART(HOUR,@Time) 
          END AS Hour_Code 
		, RIGHT('00'+RTRIM(CASE 
		    WHEN DATEPART(HOUR,@Time) = 0 THEN 12
            WHEN DATEPART(HOUR,@Time) > 12 THEN DATEPART(HOUR,@Time) - 12 
            ELSE DATEPART(HOUR,@Time) 
          END),2) AS Hour_name 
        , DATEPART(HOUR,@Time) [Hour24_code] 
		, RIGHT('00'+RTRIM(DATEPART(HOUR,@Time)),2) [Hour24_name] 
        , DATEPART(MINUTE,@Time) [Minute_code] 
		, RIGHT('00'+RTRIM(DATEPART(MINUTE,@Time)),2) [Minute_name] 
		, [Minute24_code] = DATEDIFF(MINUTE,'00:00:00',@Time)
		, [Minute24_name] = DATEDIFF(MINUTE,'00:00:00',@Time)
        , DATEPART(SECOND,@Time) [Second_code] 
		, RIGHT('00'+RTRIM(DATEPART(SECOND,@Time)),2) [Second_name] 
		, [Second24_code] = DATEDIFF(SECOND,'00:00:00',@Time)
		, [Second24_name] = DATEDIFF(SECOND,'00:00:00',@Time)
        , CASE 
            WHEN DATEPART(HOUR,@Time) >= 12 THEN 'PM' 
            ELSE 'AM' 
        END AS [AmPm]
		, RIGHT('00'+RTRIM(CASE 
		    WHEN DATEPART(HOUR,@Time) = 0 THEN 12
            WHEN DATEPART(HOUR,@Time) > 12 THEN DATEPART(HOUR,@Time) - 12
            ELSE DATEPART(HOUR,@Time) 
          END),2) +':'+
		  RIGHT('00'+RTRIM(DATEPART(MINUTE,@Time)),2)  +':'+
		  RIGHT('00'+RTRIM(DATEPART(SECOND,@Time)),2) +' '+
		  CASE 
            WHEN DATEPART(HOUR,@Time) >= 12 THEN 'PM' 
            ELSE 'AM' 
          END AS [StandardTime]
		, 0 AS [ETLBatchID_Insert]
		, 0 AS [ETLBatchID_Update]
 SET @TIME = DATEADD(SECOND,1,@Time)
 SET @i=@i+1
END

RETURN 0
