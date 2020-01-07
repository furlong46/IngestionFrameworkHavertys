CREATE TABLE [DW].[Fact_SpecialOrderAttributesKeyValuesBridge] (
    [Special_Order_Attributes_Key_Values_Bridge_sk] INT            IDENTITY (1, 1) NOT NULL,
    [Attribute_Value]                               VARCHAR (255)  NULL,
    [Vendor_Value]                                  VARCHAR (255)  NULL,
    [Grade_Value]                                   VARCHAR (255)  NULL,
    [Special_Order_Attributes_Key_fk]               INT            NOT NULL,
    [SKU_fk]                                        INT            NOT NULL,
    [Attribute]                                     VARCHAR (1000) NULL,
    [Attribute_Group]                               VARCHAR (50)   NULL,
    [SourceSystem_fk]                               INT            NOT NULL,
    [ETLBatchID_Insert]                             INT            NOT NULL,
    [ETLBatchID_Update]                             INT            NOT NULL,
    CONSTRAINT [FK_Fact_SpecialOrderAttributesKeyValuesBridge_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_SpecialOrderAttributesKeyValuesBridge_Dim_SKU_SpecialOrderAttributesKey] FOREIGN KEY ([Special_Order_Attributes_Key_fk]) REFERENCES [DW].[Dim_SKU_SpecialOrderAttributesKey] ([Special_Order_Attributes_Key_sk])
);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_SpecialOrderAttributesKeyValuesBridge]
    ON [DW].[Fact_SpecialOrderAttributesKeyValuesBridge];

