CREATE TABLE [ETL].[JobMaster] (
    [MasterProcessNumber] INT          IDENTITY (1, 1) NOT NULL,
    [JobConfiguration]    VARCHAR (50) NOT NULL,
    [Status]              VARCHAR (50) NULL,
    [StartTime]           DATETIME     NOT NULL,
    [ReStartTime]         DATETIME     NULL,
    [EndTime]             DATETIME     CONSTRAINT [DF_JobMaster_EndTime] DEFAULT (getdate()) NULL,
    CONSTRAINT [PK_JobMaster] PRIMARY KEY CLUSTERED ([MasterProcessNumber] ASC)
);



