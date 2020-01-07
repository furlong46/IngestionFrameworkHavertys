CREATE TABLE [DW].[Dim_SKU] (
    [SKU_sk]                    INT             IDENTITY (1, 1) NOT NULL,
    [SKU_Unformatted_nk]        VARCHAR (9)     NOT NULL,
    [SKU]                       VARCHAR (12)    NOT NULL,
    [SKU_Description]           VARCHAR (30)    NULL,
    [Vendor_Color_ID]           INT             NULL,
    [Vendor_Color]              VARCHAR (100)   NULL,
    [Vendor_Color_Hex_Code]     VARCHAR (10)    NULL,
    [Vendor_Finish_ID]          INT             NULL,
    [Vendor_Finish]             VARCHAR (100)   NULL,
    [Configuration_ID]          INT             NULL,
    [Configuration]             VARCHAR (100)   NULL,
    [Display_Configuration]     VARCHAR (100)   NULL,
    [Size_ID]                   INT             NULL,
    [Size]                      VARCHAR (100)   NULL,
    [Original_Vendor_ID]        VARCHAR (3)     NOT NULL,
    [Original_Vendor_fk]        INT             NOT NULL,
    [Collection_ID]             INT             NOT NULL,
    [Collection]                VARCHAR (40)    NOT NULL,
    [Group_ID]                  INT             NOT NULL,
    [Group]                     VARCHAR (20)    NOT NULL,
    [Product_ID]                INT             NOT NULL,
    [Product]                   VARCHAR (100)   NOT NULL,
    [Reporting_Class_Code]      VARCHAR (2)     NOT NULL,
    [Reporting_Class]           VARCHAR (50)    NOT NULL,
    [Super_Category_ID]         INT             NOT NULL,
    [Super_Category]            VARCHAR (100)   NOT NULL,
    [Category_ID]               INT             NOT NULL,
    [Category]                  VARCHAR (100)   NOT NULL,
    [Sub_Category_ID]           INT             NOT NULL,
    [Sub_Category]              VARCHAR (100)   NOT NULL,
    [Current_EDLP_Price]        DECIMAL (10, 4) NOT NULL,
    [Good_Better_Best_Category] VARCHAR (50)    NOT NULL,
    [GBB_Price_Point]           VARCHAR (50)    NULL,
    [Motion]                    VARCHAR (50)    NOT NULL,
    [Dining_Style]              VARCHAR (50)    NULL,
    [Bedroom_Style]             VARCHAR (50)    NULL,
    [Standalone_bed]            VARCHAR (50)    NOT NULL,
    [SKU_Type]                  VARCHAR (1)     NOT NULL,
    [Stocked]                   VARCHAR (1)     NOT NULL,
    [Type_of_SKU_Code]          VARCHAR (1)     NOT NULL,
    [Type_of_SKU]               VARCHAR (50)    NOT NULL,
    [Vendor_ID]                 VARCHAR (3)     NOT NULL,
    [Vendor_fk]                 INT             NOT NULL,
    [Factory_ID]                INT             NOT NULL,
    [Factory_fk]                INT             NOT NULL,
    [Special_Order_Available]   VARCHAR (1)     NOT NULL,
    [Vendor_Lead_Time]          INT             NULL,
    [Product_Lead_Time]         INT             NULL,
    [Group_Active_Start]        DATE            NULL,
    [Group_Active_End]          DATE            NULL,
    [SKU_Flow_StartDate]        DATE            NULL,
    [SKU_Flow_EndDate]          DATE            NULL,
    [Stocked_at_EDC]            INT             NOT NULL,
    [Stocked_at_WDC]            INT             NOT NULL,
    [Stocked_at_FDC]            INT             NOT NULL,
    [Group_Web_Status]          INT             NOT NULL,
    [SKU_Web_Status]            INT             NOT NULL,
    [Merchandising_Approval]    VARCHAR (1)     NOT NULL,
    [Marketing_Approval]        VARCHAR (1)     NOT NULL,
    [Lineup_Status_Code]        VARCHAR (1)     NOT NULL,
    [Lineup_Status]             VARCHAR (50)    NOT NULL,
    [Available_to_Purchase]     VARCHAR (50)    NOT NULL,
    [Ship_Length]               INT             NOT NULL,
    [Ship_Width]                INT             NOT NULL,
    [Ship_Height]               INT             NOT NULL,
    [Length]                    INT             NOT NULL,
    [Width]                     INT             NOT NULL,
    [Height]                    INT             NOT NULL,
    [Weight]                    DECIMAL (5, 1)  NOT NULL,
    [Prep_Cubes]                DECIMAL (5, 1)  NOT NULL,
    [Cubes]                     DECIMAL (5, 1)  NOT NULL,
    [Prep_Flag]                 VARCHAR (1)     NOT NULL,
    [Assembly_Required]         VARCHAR (1)     NOT NULL,
    [Assembly_Time]             DECIMAL (4, 1)  NOT NULL,
    [Leave_in_Box]              VARCHAR (1)     NOT NULL,
    [Manufacturer_ID]           VARCHAR (1000)  NOT NULL,
    [UPC]                       BIGINT          NULL,
    [HTS_Number]                BIGINT          NULL,
    [Original_SKU]              VARCHAR (9)     NOT NULL,
    [Representative_Item]       VARCHAR (9)     NOT NULL,
    [Group_Rating_Count]        INT             NOT NULL,
    [Product_Rating_Count]      INT             NOT NULL,
    [Group_Star_Rating]         DECIMAL (6, 4)  NOT NULL,
    [Product_Star_Rating]       DECIMAL (6, 4)  NOT NULL,
    [Made_in_ID]                INT             NOT NULL,
    [Made_in_City]              VARCHAR (25)    NOT NULL,
    [Made_in_State]             VARCHAR (2)     NOT NULL,
    [Made_in_Country]           VARCHAR (2)     NOT NULL,
    [Origin_ID]                 INT             NOT NULL,
    [Origin_City]               VARCHAR (25)    NOT NULL,
    [Origin_State]              VARCHAR (2)     NOT NULL,
    [Origin_Country]            VARCHAR (2)     NOT NULL,
    [Copy]                      VARCHAR (5000)  NOT NULL,
    [Web_Description]           VARCHAR (5000)  NOT NULL,
    [Price_Tag_Description]     VARCHAR (5000)  NOT NULL,
    [Primary_Common_Color]      VARCHAR (50)    NOT NULL,
    [Primary_Common_Finish]     VARCHAR (50)    NOT NULL,
    [Floor_Sample_Bedding]      VARCHAR (50)    NOT NULL,
    [Employee_Bedding]          VARCHAR (50)    NOT NULL,
    [Accessory_Story]           VARCHAR (50)    NOT NULL,
    [IndirectDirect]            VARCHAR (9)     NULL,
    [3D_Model_Required]         VARCHAR (50)    NOT NULL,
    [3D_Model]                  VARCHAR (50)    NOT NULL,
    [Scene_7_Image_URL]         VARCHAR (100)   NOT NULL,
    [Havertys.com_URL]          VARCHAR (100)   NOT NULL,
    [Advertised_Price]          DECIMAL (7, 2)  NOT NULL,
    [HashKey]                   CHAR (40)       NULL,
    [SourceSystem_fk]           INT             NOT NULL,
    [ETLBatchID_Insert]         INT             NOT NULL,
    [ETLBatchID_Update]         INT             NOT NULL,
    CONSTRAINT [PK_SKU_sk] PRIMARY KEY CLUSTERED ([SKU_sk] ASC) WITH (DATA_COMPRESSION = PAGE),
    CONSTRAINT [FK_Dim_SKU_Dim_Factory] FOREIGN KEY ([Factory_fk]) REFERENCES [DW].[Dim_Factory] ([Factory_sk]),
    CONSTRAINT [FK_Dim_SKU_Dim_Vendor] FOREIGN KEY ([Vendor_fk]) REFERENCES [DW].[Dim_Vendor] ([Vendor_sk]),
    CONSTRAINT [FK_Dim_SKU_Dim_Vendor_Orig] FOREIGN KEY ([Original_Vendor_fk]) REFERENCES [DW].[Dim_Vendor] ([Vendor_sk])
);
















GO







GO
CREATE NONCLUSTERED INDEX [IX_SKU_sk_SKU_Unformatted_sk_HashKey_ETLBatchID_Insert]
    ON [DW].[Dim_SKU]([SKU_sk] ASC, [SKU_Unformatted_nk] ASC, [HashKey] ASC, [ETLBatchID_Insert] ASC) WITH (DATA_COMPRESSION = PAGE);






GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_SKU_Unformatted_nk]
    ON [DW].[Dim_SKU]([SKU_Unformatted_nk] ASC) WITH (DATA_COMPRESSION = PAGE);

