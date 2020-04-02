CREATE TABLE [DW].[Dim_OverrideReason] (
    [Override_Reason_sk]          INT           IDENTITY (1, 1) NOT NULL,
    [Override_Reason_ID]          INT           NOT NULL,
    [Override_Reason]             VARCHAR (50)  NOT NULL,
    [Override_Reason_Text]        VARCHAR (400) NOT NULL,
    [Override_Approve_First_Name] VARCHAR (15)  NOT NULL,
    [Override_Approve_Last_Name]  VARCHAR (20)  NOT NULL,
    [HashKey]                     CHAR (40)     NULL,
    [SourceSystem_fk]             INT           NOT NULL,
    [ETLBatchID_Insert]           INT           NOT NULL,
    [ETLBatchID_Update]           INT           NOT NULL,
    CONSTRAINT [PK_Override_Reason_sk] PRIMARY KEY CLUSTERED ([Override_Reason_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);





