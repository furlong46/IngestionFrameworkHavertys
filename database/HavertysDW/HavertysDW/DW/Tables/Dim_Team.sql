CREATE TABLE [DW].[Dim_Team] (
    [Team_sk]           INT           IDENTITY (1, 1) NOT NULL,
    [TeamMember1_ID_nk] INT           NULL,
    [TeamMember1_Name]  VARCHAR (100) NULL,
    [TeamMember2_ID_nk] INT           NULL,
    [TeamMember2_Name]  VARCHAR (100) NULL,
    [TeamType_nk]       VARCHAR (25)  NOT NULL,
    [HashKey]           CHAR (40)     NOT NULL,
    [SourceSystem_fk]   INT           NOT NULL,
    [ETLBatchID_Insert] INT           NOT NULL,
    [ETLBatchID_Update] INT           NOT NULL,
    CONSTRAINT [PK_Team_sk] PRIMARY KEY CLUSTERED ([Team_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);







