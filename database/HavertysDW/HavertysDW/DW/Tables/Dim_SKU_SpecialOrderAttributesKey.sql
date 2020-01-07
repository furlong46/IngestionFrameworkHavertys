CREATE TABLE [DW].[Dim_SKU_SpecialOrderAttributesKey] (
    [Special_Order_Attributes_Key_sk] INT            IDENTITY (1, 1) NOT NULL,
    [Attribute_Type_ID_nk]            INT            NOT NULL,
    [Attribute]                       VARCHAR (1000) NOT NULL,
    [Attribute_Group]                 VARCHAR (50)   NOT NULL,
    [HashKey]                         CHAR (40)      NULL,
    [SourceSystem_fk]                 INT            NOT NULL,
    [ETLBatchID_Insert]               INT            NOT NULL,
    [ETLBatchID_Update]               INT            NOT NULL,
    CONSTRAINT [PK_Special_Order_Attributes_Key_Values_sk] PRIMARY KEY CLUSTERED ([Special_Order_Attributes_Key_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);



