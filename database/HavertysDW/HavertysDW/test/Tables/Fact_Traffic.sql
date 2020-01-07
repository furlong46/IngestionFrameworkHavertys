CREATE TABLE [test].[Fact_Traffic] (
    [Traffic_sk]        INT          NULL,
    [Staff_on_Floor]    INT          NOT NULL,
    [Ups]               INT          NOT NULL,
    [Call_Customer_Ups] INT          NOT NULL,
    [Raw_Traffic_Count] INT          NOT NULL,
    [Traffic_Count]     INT          NOT NULL,
    [Date_fk]           INT          NOT NULL,
    [Hour_fk]           INT          NOT NULL,
    [Location_fk]       INT          NOT NULL,
    [Location_Code_nk]  VARCHAR (15) NULL,
    [Hour_code]         TINYINT      NULL,
    [SourceSystem_fk]   INT          NOT NULL,
    [ETLBatchID_Insert] INT          NOT NULL,
    [ETLBatchID_Update] INT          NOT NULL
);

