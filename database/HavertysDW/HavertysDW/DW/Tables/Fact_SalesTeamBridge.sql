CREATE TABLE [DW].[Fact_SalesTeamBridge] (
    [Sales_Team_Bridge_sk] INT            IDENTITY (1, 1) NOT NULL,
    [Count]                INT            NULL,
    [Allocation]           DECIMAL (3, 2) NULL,
    [Sales_Team_fk]        INT            NOT NULL,
    [Sales_Associate_fk]   INT            NOT NULL,
    [TeamMember1_ID_nk]    INT            NOT NULL,
    [TeamMember2_ID_nk]    INT            NOT NULL,
    [Employee_ID_nk]       INT            NOT NULL,
    [SourceSystem_fk]      INT            NOT NULL,
    [ETLBatchID_Insert]    INT            NOT NULL,
    [ETLBatchID_Update]    INT            NOT NULL,
    CONSTRAINT [PK_Sales_Team_Bridge_sk] PRIMARY KEY CLUSTERED ([Sales_Team_Bridge_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_SalesTeamBridge_Dim_Employee] FOREIGN KEY ([Sales_Associate_fk]) REFERENCES [DW].[Dim_Employee] ([Employee_sk]),
    CONSTRAINT [FK_Fact_SalesTeamBridge_Dim_Team] FOREIGN KEY ([Sales_Team_fk]) REFERENCES [DW].[Dim_Team] ([Team_sk])
);







