CREATE TABLE [Updates].[Dim_Team_Update] (
    [TeamMember1_ID_nk]      DECIMAL (38, 18) NULL,
    [TeamMember1_Name]       NVARCHAR (MAX)   NOT NULL,
    [TeamMember2_ID_nk]      DECIMAL (38, 18) NULL,
    [TeamMember2_Name]       NVARCHAR (MAX)   NOT NULL,
    [TeamType_nk]            NVARCHAR (MAX)   NULL,
    [HashKey]                NVARCHAR (MAX)   NULL,
    [SourceSystem_fk]        INT              NOT NULL,
    [ETLBatchID_Insert]      INT              NOT NULL,
    [ETLBatchID_Update]      INT              NOT NULL,
    [Team_sk_Dest]           INT              NULL,
    [TeamMember1_ID_nk_Dest] INT              NULL,
    [TeamMember2_ID_nk_Dest] INT              NULL,
    [TeamType_nk_Dest]       NVARCHAR (MAX)   NULL,
    [HashKey_Dest]           NVARCHAR (MAX)   NULL,
    [ETLBatchID_Insert_Dest] INT              NULL
)
WITH (DATA_COMPRESSION = PAGE);



