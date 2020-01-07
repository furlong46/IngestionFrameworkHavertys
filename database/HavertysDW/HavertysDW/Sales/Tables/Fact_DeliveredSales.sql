CREATE TABLE [Sales].[Fact_DeliveredSales] (
    [Delivered_Sales_sk]               INT            IDENTITY (1, 1) NOT NULL,
    [Gross_Sales]                      DECIMAL (7, 2) NULL,
    [Group_SKU_Discount]               DECIMAL (7, 2) NULL,
    [Retail_Reduction_Discount]        DECIMAL (7, 2) NULL,
    [Override_Discount]                DECIMAL (7, 2) NULL,
    [Promotional_Discount]             DECIMAL (7, 2) NULL,
    [Unclassified_Discount]            DECIMAL (7, 2) NULL,
    [Undelivered_Allowance]            DECIMAL (7, 2) NULL,
    [Total_Discount]                   DECIMAL (7, 2) NULL,
    [Retail_Sales]                     DECIMAL (9, 2) NULL,
    [Tax_Amount_1]                     DECIMAL (7, 2) NULL,
    [Tax_Amount_2]                     DECIMAL (7, 2) NULL,
    [Sales_Units]                      INT            NULL,
    [Sales_Units_GroupSKU]             INT            NULL,
    [Landed_Cost_of_Goods_Sold]        DECIMAL (9, 2) NULL,
    [Gross_Profit_$]                   DECIMAL (9, 2) NULL,
    [Days_to_Deliver]                  INT            NULL,
    [Written_Date_fk]                  INT            NOT NULL,
    [Delivered_Date_fk]                INT            NOT NULL,
    [Location_fk]                      INT            NOT NULL,
    [Delivery_DC_fk]                   INT            NOT NULL,
    [Originating_DC_fk]                INT            NOT NULL,
    [SKU_fk]                           INT            NOT NULL,
    [SKU_Group_fk]                     INT            NOT NULL,
    [Customer_fk]                      INT            NOT NULL,
    [Sales_Team_fk]                    INT            NOT NULL,
    [Designer_Team_fk]                 INT            NOT NULL,
    [Merchandising_Promotion_fk]       INT            NOT NULL,
    [Credit_Promotion_fk]              INT            NOT NULL,
    [Override_Reason_fk]               INT            NOT NULL,
    [Transaction_Number_nk]            INT            NOT NULL,
    [Transaction_Line_Number_nk]       INT            NOT NULL,
    [Transaction_Line_Number_GroupSKU] INT            NULL,
    [Market_nk]                        INT            NOT NULL,
    [Sales_Status]                     VARCHAR (50)   NULL,
    [Original_Transaction_Number]      INT            NULL,
    [Sale_Type_nk]                     VARCHAR (2)    NOT NULL,
    [Transaction_Type_nk]              VARCHAR (10)   NOT NULL,
    [Transaction_Sequence_nk]          INT            NOT NULL,
    [Charge_Type]                      VARCHAR (1)    NULL,
    [Exchanged_Type]                   VARCHAR (1)    NULL,
    [Worksheet]                        VARCHAR (25)   NULL,
    [Internet_Sale_Flag]               VARCHAR (1)    NULL,
    [LP]                               VARCHAR (25)   NULL,
    [Cancel_reason]                    VARCHAR (25)   NULL,
    [Delivery_Type]                    VARCHAR (25)   NULL,
    [Omnichannel_Flag]                 VARCHAR (1)    NULL,
    [Origination]                      VARCHAR (1)    NULL,
    [Buy_Online_Pick_Up_In_Store_Flag] VARCHAR (1)    NULL,
    [SourceSystem_fk]                  INT            NOT NULL,
    [ETLBatchID_Insert]                INT            NOT NULL,
    [ETLBatchID_Update]                INT            NOT NULL,
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_CreditPromotion] FOREIGN KEY ([Credit_Promotion_fk]) REFERENCES [DW].[Dim_CreditPromotion] ([Credit_Promotion_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Customer] FOREIGN KEY ([Customer_fk]) REFERENCES [DW].[Dim_Customer] ([Customer_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Date_Delivered] FOREIGN KEY ([Delivered_Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Date_Written] FOREIGN KEY ([Written_Date_fk]) REFERENCES [DW].[Dim_Date] ([Date_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Location] FOREIGN KEY ([Location_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Location_Delivery_DC] FOREIGN KEY ([Delivery_DC_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Location_Originating_DC] FOREIGN KEY ([Originating_DC_fk]) REFERENCES [DW].[Dim_Location] ([Location_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_MerchandisingPromotion] FOREIGN KEY ([Merchandising_Promotion_fk]) REFERENCES [DW].[Dim_MerchandisingPromotion] ([Merchandising_Promotion_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_OverrideReason] FOREIGN KEY ([Override_Reason_fk]) REFERENCES [DW].[Dim_OverrideReason] ([Override_Reason_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_SKU] FOREIGN KEY ([SKU_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_SKU_Group] FOREIGN KEY ([SKU_Group_fk]) REFERENCES [DW].[Dim_SKU] ([SKU_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Team_Designer] FOREIGN KEY ([Designer_Team_fk]) REFERENCES [DW].[Dim_Team] ([Team_sk]),
    CONSTRAINT [FK_Fact_DeliveredSales_Dim_Team_Sales] FOREIGN KEY ([Sales_Team_fk]) REFERENCES [DW].[Dim_Team] ([Team_sk])
);










GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Fact_DeliveredSales]
    ON [Sales].[Fact_DeliveredSales];









