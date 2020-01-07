CREATE TABLE [Sales].[Fact_CompSalesMatrix] (
    [Comp_Sales_Matrix_sk]     INT          IDENTITY (1, 1) NOT NULL,
    [Count]                    INT          NULL,
    [Location_Accounting_Unit] VARCHAR (10) NOT NULL,
    [Date_fk]                  INT          NOT NULL,
    [Comp_Code]                VARCHAR (25) NULL,
    [Come_Name]                VARCHAR (50) NULL,
    [SourceSystem_fk]          INT          NOT NULL,
    [ETLBatchID_Insert]        INT          NOT NULL,
    [ETLBatchID_Update]        INT          NOT NULL,
    CONSTRAINT [PK_Comp_Sales_Matrix_sk] PRIMARY KEY CLUSTERED ([Comp_Sales_Matrix_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_CompSalesMatrix_Dim_Date] FOREIGN KEY ([Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk])
);



