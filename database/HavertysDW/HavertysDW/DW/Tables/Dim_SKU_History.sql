CREATE TABLE [DW].[Dim_SKU_History] (
    [SKU_History_sk]          INT           IDENTITY (1, 1) NOT NULL,
    [SKU_Unformatted_nk]      VARCHAR (9)   NOT NULL,
    [SKU]                     VARCHAR (12)  NOT NULL,
    [SKU_Description]         VARCHAR (30)  NULL,
    [Lineup_Status_Code]      VARCHAR (1)   NULL,
    [Lineup_Status]           VARCHAR (50)  NULL,
    [Analyst_Employee_Number] VARCHAR (50)  NULL,
    [Analyst_Name]            VARCHAR (100) NULL,
    [Start_Date]              DATE          NOT NULL,
    [End_Date]                DATE          NOT NULL,
    [Current_Flag]            BIT           NOT NULL,
    [HashKey]                 CHAR (40)     NOT NULL,
    [SourceSystem_fk]         INT           NOT NULL,
    [ETLBatchID_Insert]       INT           NOT NULL,
    [ETLBatchID_Update]       INT           NOT NULL,
    CONSTRAINT [PK_SKU_History_sk] PRIMARY KEY CLUSTERED ([SKU_History_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);

