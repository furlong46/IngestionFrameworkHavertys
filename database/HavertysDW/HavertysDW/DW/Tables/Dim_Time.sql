CREATE TABLE [DW].[Dim_Time] (
    [Time_sk]           INT          IDENTITY (1, 1) NOT NULL,
    [Time24_code]       TIME (0)     NULL,
    [Time24_name]       VARCHAR (8)  NOT NULL,
    [Hour_code]         TINYINT      NOT NULL,
    [Hour_name]         VARCHAR (2)  NOT NULL,
    [Hour24_code]       TINYINT      NOT NULL,
    [Hour24_name]       VARCHAR (2)  NOT NULL,
    [Minute_code]       TINYINT      NOT NULL,
    [Minute_name]       VARCHAR (2)  NOT NULL,
    [Minute24_code]     SMALLINT     NOT NULL,
    [Minute24_name]     VARCHAR (4)  NOT NULL,
    [Second_code]       TINYINT      NOT NULL,
    [Second_name]       VARCHAR (2)  NOT NULL,
    [Second24_code]     INT          NOT NULL,
    [Second24_name]     VARCHAR (5)  NOT NULL,
    [AmPm]              VARCHAR (2)  NOT NULL,
    [StandardTime]      VARCHAR (11) NOT NULL,
    [ETLBatchID_Insert] INT          NOT NULL,
    [ETLBatchID_Update] INT          NOT NULL,
    CONSTRAINT [PK_Time_sk] PRIMARY KEY CLUSTERED ([Time_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [AK_Time24_code] UNIQUE NONCLUSTERED ([Time24_code] ASC) WITH (DATA_COMPRESSION = PAGE)
);







