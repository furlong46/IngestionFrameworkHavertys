CREATE TABLE [Inventory].[Fact_InventorySnapshot] (
    [Inventory_Snapshot_sk]  INT             IDENTITY (1, 1) NOT NULL,
    [Inventory_Quantity]     INT             NULL,
    [Inventory_Cost]         DECIMAL (15, 2) NULL,
    [Damaged_Quantity]       INT             NULL,
    [Damaged_Cost]           DECIMAL (15, 2) NULL,
    [Snapshot_Date_fk]       INT             NOT NULL,
    [SKU_fk]                 INT             NOT NULL,
    [SKU_History_fk]         INT             NOT NULL,
    [Location_fk]            INT             NOT NULL,
    [Record_Type]            VARCHAR (1)     NULL,
    [Data_Aggregation_Level] VARCHAR (2)     NULL,
    [Data_Version]           INT             NULL,
    [SourceSystem_fk]        INT             NOT NULL,
    [ETLBatchID_Insert]      INT             NOT NULL,
    [ETLBatchID_Update]      INT             NOT NULL,
    CONSTRAINT [PK_Inventory_Snapshot_sk] PRIMARY KEY NONCLUSTERED ([Inventory_Snapshot_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Fact_InventorySnapshot_Dim_Date] FOREIGN KEY ([Snapshot_Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_InventorySnapshot_Dim_Location] FOREIGN KEY ([Location_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk]),
    CONSTRAINT [FK_Fact_InventorySnapshot_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_InventorySnapshot_Dim_SKU_History] FOREIGN KEY ([SKU_History_fk]) REFERENCES [DW].[Dim_SKU_History] ([SKU_History_sk])
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_InventorySnapshot]
    ON [Inventory].[Fact_InventorySnapshot];

