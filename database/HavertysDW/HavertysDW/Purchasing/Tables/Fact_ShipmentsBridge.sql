CREATE TABLE [Purchasing].[Fact_ShipmentsBridge] (
    [Shipments_Bridge_sk]         INT       IDENTITY (1, 1) NOT NULL,
    [Count]                       INT       NULL,
    [Shipments_fk]                INT       NOT NULL,
    [SKU_fk]                      INT       NOT NULL,
    [Transaction_Link_fk]         INT       NOT NULL,
    [Shipment_Number_nk]          INT       NULL,
    [Shipment_Sequence_Number_nk] INT       NULL,
    [Division_nk]                 INT       NULL,
    [Market_nk]                   INT       NULL,
    [Docking_Number_nk]           INT       NULL,
    [PO_Number]                   INT       NULL,
    [PO_Line_Number]              SMALLINT  NULL,
    [Receive_Sequence_Number]     SMALLINT  NULL,
    [HashKey]                     CHAR (40) NOT NULL,
    [SourceSystem_fk]             INT       NOT NULL,
    [ETLBatchID_Insert]           INT       NOT NULL,
    [ETLBatchID_Update]           INT       NOT NULL,
    CONSTRAINT [PK_Shipments_Bridge_sk] PRIMARY KEY NONCLUSTERED ([Shipments_Bridge_sk] ASC),
    CONSTRAINT [FK_Fact_ShipmentsBridge_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_ShipmentsBridge_Fact_ShipmentsTemporalSnapshots] FOREIGN KEY ([Shipments_fk]) REFERENCES [Purchasing].[Fact_ShipmentsTemporalSnapshots] ([Shipments_sk])
);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_ShipmentsBridge]
    ON [Purchasing].[Fact_ShipmentsBridge];

