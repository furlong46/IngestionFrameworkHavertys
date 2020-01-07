CREATE TABLE [Sales].[Fact_Traffic] (
    [Traffic_sk]        INT          IDENTITY (1, 1) NOT NULL,
    [Staff_on_Floor]    INT          NULL,
    [Ups]               INT          NULL,
    [Call_Customer_Ups] INT          NULL,
    [Raw_Traffic_Count] INT          NULL,
    [Traffic_Count]     INT          NULL,
    [Date_fk]           INT          NOT NULL,
    [Hour_fk]           INT          NOT NULL,
    [Location_fk]       INT          NOT NULL,
    [Location_Code_nk]  VARCHAR (15) NULL,
    [Hour_code]         TINYINT      NULL,
    [SourceSystem_fk]   INT          NOT NULL,
    [ETLBatchID_Insert] INT          NOT NULL,
    [ETLBatchID_Update] INT          NOT NULL,
    CONSTRAINT [FK_Fact_Traffic_Dim_Date] FOREIGN KEY ([Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_Traffic_Dim_Location] FOREIGN KEY ([Location_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk]),
    CONSTRAINT [FK_Fact_Traffic_Dim_Time] FOREIGN KEY ([Hour_fk]) REFERENCES [DW].[Dim_Time] ([Time_sk])
);










GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_Traffic]
    ON [Sales].[Fact_Traffic];

