SET IDENTITY_INSERT [DW].[Dim_CreditPromotion] ON 
GO
INSERT [DW].[Dim_CreditPromotion] ([Credit_Promotion_sk], [Credit_Promotion_Code_nk], [Credit_Promotion_Description], [Minimum_Sale_Amount], [Down_Payment_Percentage], [Plan_Name], [Plan_Description], [Duration], [Promo_APR], [Purchase_APR], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'0', N'Failed LookUp', CAST(0.00 AS Decimal(8, 2)), 0, N'', N'', 0, CAST(0.00 AS Decimal(4, 2)), CAST(0.00 AS Decimal(4, 2)), NULL, 0, 0, 0)

INSERT [DW].[Dim_CreditPromotion] ([Credit_Promotion_sk], [Credit_Promotion_Code_nk], [Credit_Promotion_Description], [Minimum_Sale_Amount], [Down_Payment_Percentage], [Plan_Name], [Plan_Description], [Duration], [Promo_APR], [Purchase_APR], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, N'-1', N'No Credit Promotion', CAST(0.00 AS Decimal(8, 2)), 0, N'', N'', 0, CAST(0.00 AS Decimal(4, 2)), CAST(0.00 AS Decimal(4, 2)), NULL, 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_CreditPromotion] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Customer] ON 
GO
INSERT [DW].[Dim_Customer] ([Customer_sk], [Customer_Sequence_nk], [Customer_ID_nk], [Sequence_Status], [Bill_First_Name], [Bill_Last_Name], [Bill_Name_Status], [Bill_Address], [Bill_City], [Bill_State], [Bill_ZIP], [Bill_ZIP4], [Bill_Latitude], [Bill_Longitude], [Bill_Address_Status], [Delivery_First_Name], [Delivery_Last_Name], [Delivery_Name_Status], [Delivery_Address], [Delivery_City], [Delivery_State], [Delivery_ZIP], [Delivery_ZIP4], [Delivery_Latitude], [Delivery_Longitude], [Delivery_Address_Status], [Customer_Profile_Created_Date], [Customer_First_Name], [Customer_Last_Name], [Customer_Name_Status], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZIP], [Customer_ZIP4], [Customer_Latitude], [Customer_Longitude], [Customer_Phone_Type], [Customer_Phone], [Customer_Phone_Extension], [Customer_Phone_Status], [Customer_Email], [Customer_Email_Status], [Customer_Created_Date], [Customer_Status], [No_Email], [Room_Plan], [Send_Advertising], [Send_Catalog], [Send_News], [Send_P_Mail], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, 0, N'', N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', CAST(N'1900-01-01' AS Date), N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', N'', N'', N'', N'', N'', CAST(N'1900-01-01' AS Date), N'', N'', N'', N'', N'', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Customer] OFF
GO
INSERT [DW].[Dim_Date] ([Date_sk], [Isworkday_Store], [Isworkday_Delivery], [Calendar_date], [Calendar_name_sht], [Calendar_name_lng], [Calendar_year_day_number], [Calendar_year_day_name], [Calendar_quarter_day_number], [Calendar_quarter_day_name], [Calendar_month_day_number], [Calendar_month_day_name], [Calendar_weekday_number], [Calendar_weekday_code], [Calendar_weekday_name], [Calendar_week_of_month_number], [Calendar_week_of_month_name], [Calendar_week_of_year_number], [Calendar_week_of_year_name], [Calendar_weekbegin_date], [Calendar_weekending_date], [Calendar_weekending_name], [Calendar_month_number], [Calendar_month_code], [Calendar_month_name], [Calendar_quarter_number], [Calendar_quarter_code], [Calendar_quarter_name], [Calendar_month_week_year_number], [Calendar_month_week_year_code], [Calendar_month_year_number], [Calendar_month_year_code], [Calendar_quarter_year_number], [Calendar_quarter_year_code], [Calendar_quarter_year_name], [Calendar_year_number], [Calendar_year_name], [Reporting_week_number], [Reporting_weekending_date], [Reporting_weekending_name], [Reporting_weekday_number], [Reporting_month_number], [Reporting_month_code], [Reporting_month_name], [Reporting_quarter_number], [Reporting_quarter_code], [Reporting_quarter_name], [Reporting_month_year_number], [Reporting_month_year_code], [Reporting_quarter_year_number], [Reporting_quarter_year_code], [Reporting_quarter_year_name], [Reporting_year_number], [Reporting_year_name], [Holiday_name], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, 0, CAST(N'1900-01-01' AS Date), N'', N'', 0, N'', 0, N'', 0, N'', 0, N'   ', N'', 0, N'', 0, N'', CAST(N'1899-12-31' AS Date), CAST(N'1900-01-06' AS Date), N'WE  Jan 06', 0, N'   ', N'', 0, N'  ', N'', 0, N'', 0, N'', 0, N'', N'', 0, N'', 0, CAST(N'1900-01-01' AS Date), N'', 0, 0, N'   ', N'', 0, N'  ', N'', 0, N'', 0, N'', N'', 0, N'', N'', NULL, NULL)
GO
SET IDENTITY_INSERT [DW].[Dim_Employee] ON 
GO
INSERT [DW].[Dim_Employee] ([Employee_sk], [Employee_ID_nk], [EmployeeType_Code], [EmployeeType_Name], [Sales_ID], [WMS_ID], [Driver_ID], [Designer_ID], [First_Name], [Last_Name], [Full_Name], [Division_ID], [Market_ID], [Branch_ID], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'0000000000', N'E', N'Employee', 0, 0, 0, 0, N'Failed Lookup', N'Failed Lookup', N'Failed Lookup', 0, 0, 0, N'0                                       ', 0, 0, 0)
INSERT [DW].[Dim_Employee] ([Employee_sk], [Employee_ID_nk], [EmployeeType_Code], [EmployeeType_Name], [Sales_ID], [WMS_ID], [Driver_ID], [Designer_ID], [First_Name], [Last_Name], [Full_Name], [Division_ID], [Market_ID], [Branch_ID], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, N'0000000001', N'E', N'Employee', 0, 0, 0, 0, N'No Employee', N'No Employee', N'No Employee', 0, 0, 0, N'0', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Employee] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Factory] ON 
GO
INSERT [DW].[Dim_Factory] ([Factory_sk], [Factory_Code_nk], [Factory_Name], [Address_1], [Address_2], [City], [State], [Zip], [Country_Code], [Country], [Active], [Latitude], [Longitude], [Created_Date], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'', N'', N'', N'', N'0', NULL, N'', N'', NULL, NULL, NULL, N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Factory] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Location] ON 
GO
INSERT [DW].[Dim_Location] ([Location_sk], [Location_Code_nk], [Division_ID], [Division], [Region_ID], [Region], [Market_Code], [Market_ID], [Market], [Location_ID], [Location_Name], [Location_Sales_Manager], [Market_Manager], [Label], [Branch_Type], [Location_Type], [Phone_Number], [Address_1], [Address_2], [City], [State], [Country], [Zip], [Zip4], [Market_Zip], [Longitude], [Latitude], [Branch_Announced_Date], [Branch_Shut_Down_Date], [Active], [Total_Square_Feet], [Total_Sales_Square_Feet], [Sales_Conditioned_CC_Square_Feet], [Sales_Square_Feet], [Warehouse_Square_Feet], [Office_Utility_Square_Feet], [Admin_Flag], [Size], [Time_Zone], [Accounting_Unit], [Branch_Open_Date], [Branch_Close_Date], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'', 0, N'', N'', N'', 0, 0, N'', 0, N'', N'', N'', N'', N'', N'', CAST(0 AS Decimal(10, 0)), N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.0000000000000 AS Decimal(16, 13)), CAST(0.0000000000000 AS Decimal(16, 13)), CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), N'', CAST(0.0000 AS Decimal(10, 4)), CAST(0.0000 AS Decimal(10, 4)), CAST(0.0000 AS Decimal(10, 4)), CAST(0.0000 AS Decimal(10, 4)), CAST(0.0000 AS Decimal(10, 4)), CAST(0.0000 AS Decimal(10, 4)), N'', N'', N'', N'', CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Location] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_MerchandisingPromotion] ON 
GO
INSERT [DW].[Dim_MerchandisingPromotion] ([Merchandising_Promotion_sk], [Merchandising_Promotion_Key_nk], [Merchandising_Promotion_Code_nk], [Merchandising_Promotion_Description], [Type_Code], [Type], [Start_Date], [End_Date], [Segment_Code], [Segment], [Available_on_Internet], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'Failed Lookup', N'', N'', CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, N'', N'', NULL, 0, 0, 0)
INSERT [DW].[Dim_MerchandisingPromotion] ([Merchandising_Promotion_sk], [Merchandising_Promotion_Key_nk], [Merchandising_Promotion_Code_nk], [Merchandising_Promotion_Description], [Type_Code], [Type], [Start_Date], [End_Date], [Segment_Code], [Segment], [Available_on_Internet], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'', N'No Merchandising Promotion', N'', N'', CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, N'', N'', NULL, 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_MerchandisingPromotion] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_OverrideReason] ON 
GO
INSERT [DW].[Dim_OverrideReason] ([Override_Reason_sk], [Override_Reason_ID], [Override_Reason], [Override_Reason_Text], [Override_Approve_First_Name], [Override_Approve_Last_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'No Override', N'No Override', N'', N'', N'                                        ', 0, 0, 0)
GO
INSERT [DW].[Dim_OverrideReason] ([Override_Reason_sk], [Override_Reason_ID], [Override_Reason], [Override_Reason_Text], [Override_Approve_First_Name], [Override_Approve_Last_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'Failed Lookup', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_OverrideReason] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_SKU] ON 
GO
INSERT [DW].[Dim_SKU] ([SKU_sk], [SKU_Unformatted_nk], [SKU], [SKU_Description], [Vendor_Color_ID], [Vendor_Color], [Vendor_Color_Hex_Code], [Vendor_Finish_ID], [Vendor_Finish], [Configuration_ID], [Configuration], [Display_Configuration], [Size_ID], [Size], [Original_Vendor_ID], [Original_Vendor_fk], [Collection_ID], [Collection], [Group_ID], [Group], [Product_ID], [Product], [Reporting_Class_Code], [Reporting_Class], [Super_Category_ID], [Super_Category], [Category_ID], [Category], [Sub_Category_ID], [Sub_Category], [Current_EDLP_Price], [Good_Better_Best_Category], [GBB_Price_Point], [Motion], [Dining_Style], [Bedroom_Style], [Standalone_bed], [SKU_Type], [Stocked], [Type_of_SKU_Code], [Type_of_SKU], [Vendor_ID], [Vendor_fk], [Factory_ID], [Factory_fk], [Special_Order_Available], [Vendor_Lead_Time], [Product_Lead_Time], [Group_Active_Start], [Group_Active_End], [SKU_Flow_StartDate], [SKU_Flow_EndDate], [Stocked_at_EDC], [Stocked_at_WDC], [Stocked_at_FDC], [Group_Web_Status], [SKU_Web_Status], [Merchandising_Approval], [Marketing_Approval], [Lineup_Status_Code], [Lineup_Status], [Available_to_Purchase], [Ship_Length], [Ship_Width], [Ship_Height], [Length], [Width], [Height], [Weight], [Prep_Cubes], [Cubes], [Prep_Flag], [Assembly_Required], [Assembly_Time], [Leave_in_Box], [Manufacturer_ID], [UPC], [HTS_Number], [Original_SKU], [Representative_Item], [Group_Rating_Count], [Product_Rating_Count], [Group_Star_Rating], [Product_Star_Rating], [Made_in_ID], [Made_in_City], [Made_in_State], [Made_in_Country], [Origin_ID], [Origin_City], [Origin_State], [Origin_Country], [Copy], [Web_Description], [Price_Tag_Description], [Primary_Common_Color], [Primary_Common_Finish], [Floor_Sample_Bedding], [Employee_Bedding], [Accessory_Story], [IndirectDirect], [3D_Model_Required], [3D_Model], [Scene_7_Image_URL], [Havertys.com_URL], [Advertised_Price], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'000000000', N'0-0000-0000', N'Failed Lookup', 0, N'', N'', 0, N'', 0, N'', N'', 0, N'', N'', 0, 0, N'', 0, N'', 0, N'', N'', N'', 0, N'', 0, N'', 0, N'', CAST(0.0000 AS Decimal(10, 4)), N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', 0, 0, 0, N'', 0, 0, CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, 0, 0, 0, 0, N'', N'', N'', N'', N'', 0, 0, 0, 0, 0, 0, CAST(0.0 AS Decimal(5, 1)), CAST(0.0 AS Decimal(5, 1)), CAST(0.0 AS Decimal(5, 1)), N'', N'', CAST(0.0 AS Decimal(4, 1)), N'', N'', 0, 0, N'', N'', 0, 0, CAST(0.0000 AS Decimal(6, 4)), CAST(0.0000 AS Decimal(6, 4)), 0, N'', N'', N'', 0, N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', NULL, N'', N'', N'', N'', CAST(0.00 AS Decimal(7, 2)), N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_CommonFinish] ON 
GO
INSERT [DW].[Dim_SKU_CommonFinish] ([Common_Finish_sk], [Common_Finish_Code_nk], [Common_Finish_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_CommonFinish] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_Materials] ON 
GO
INSERT [DW].[Dim_SKU_Materials] ([Materials_sk], [Material_Code_nk], [Material_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'', NULL, 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_Materials] OFF
GO
INSERT [DW].[Dim_SKU_SpecialOrderAttributes] ([SKU_sk], [SKU_Unformatted_nk], [Accent_Fabric], [Arm_Type], [Armcap], [Back_and_Seat_Cushion_Core], [Back_Cushion_Attachment], [Back_Cushion_Style], [Base_Type], [Body_Fabric], [Body_Fabric_#1], [Body_Fabric_#2], [Body_Fabric_#3], [Body_Fabric_Headboard_Back], [Border_Location], [Bottom_Color], [Buttons], [Buttons_#1], [Buttons_#2], [Chair_Finish], [Chair_Seat_Finish], [Distressing], [Door_Hardware], [Drapery_Border], [Drapery_Heading], [Drapery_Length], [Drapery_Rod_Size], [Drawer_Hardware], [Fabrics], [Finish_Color], [Frame], [Fringe/Cord], [Fringe/Cord_#1], [Fringe/Cord_#2], [Fringe/Cord_#3], [Fringe/Cord_#4], [Fringe/Cord_Individual], [Joiner], [Kidney_Pillow_-_Back], [Kidney_Pillow_-_Front], [Nailhead], [Nailhead_for_Arm], [Nailhead_Spacing], [Picture_Frame_Fabric], [Pillow_Fabric-497], [Pillow_Fabric-508], [Pillow_Fabric_(Individual)], [Pillow_Fabric_(Pair)], [Pillow_Fabric_#1_Individual], [Pillow_Fabric_#1Pair], [Pillow_Fabric_#2_Individual], [Pillow_Fabric_#2_Pair], [Pillow_Fabric_#3_Individual], [Pillow_Fabric_1_-_Back_(individual)], [Pillow_Fabric_1_-_Back_(pair)], [Pillow_Fabric_1_-_Front_(individual)], [Pillow_Fabric_1_-_Front_(pair)], [Pillow_Fabric_2_-_Back_(individual)], [Pillow_Fabric_2_-_Back_(pair)], [Pillow_Fabric_2_-_Front_(individual)], [Pillow_Fabric_2_-_Front_(pair)], [Pillow_Fabric_3_-_Back_(individual)], [Pillow_Fabric_3_-_Back_(pair)], [Pillow_Fabric_3_-_Front_(individual)], [Pillow_Fabric_3_-_Front_(pair)], [Pillow_Size], [Rod_Finial], [Rug_Color_A], [Rug_Color_B], [Rug_Color_C], [Rug_Color_D], [Rug_Color_E], [Rug_Design], [Seat_Depth], [Server_Base_Finish], [Server_Top_Finish], [Solarium_Pillow_(Individual)], [Solarium_Pillow_(Pair)], [Stool_Height], [Sunbrella_Pillow_(Pair)], [Sunbrella_Pillow_#1_Individual], [Sunbrella_Pillow_#2_Individual], [Table_Apron_Finish], [Table_Leg_Finish], [Table_Top_Finish], [Top_Color], [Upholstery], [Welt], [Wing_Fabric], [7pk_Rings_with_Clips], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_SpecialOrderAttributesKey] ON 
GO
INSERT [DW].[Dim_SKU_SpecialOrderAttributesKey] ([Special_Order_Attributes_Key_sk], [Attribute_Type_ID_nk], [Attribute], [Attribute_Group], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_SpecialOrderAttributesKey] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Team] ON 
GO
INSERT [DW].[Dim_Team] ([Team_sk], [TeamMember1_ID_nk], [TeamMember1_Name], [TeamMember2_ID_nk], [TeamMember2_Name], [TeamType_nk], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', 0, N'Failed Lookup', N'Failed Lookup', N'0                                       ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Team] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Time] ON 
GO
INSERT [DW].[Dim_Time] ([Time_sk], [Time24_code], [Time24_name], [Hour_code], [Hour_name], [Hour24_code], [Hour24_name], [Minute_code], [Minute_name], [Minute24_code], [Minute24_name], [Second_code], [Second_name], [Second24_code], [Second24_name], [AmPm], [StandardTime], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, NULL, N'', 0, N'', 0, N'', 0, N'', 0, N'', 0, N'', 0, N'', N'', N'', 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Time] OFF
GO
SET IDENTITY_INSERT [DW].[Dim_Vendor] ON 
GO
INSERT [DW].[Dim_Vendor] ([Vendor_sk], [Vendor_ID_nk], [Vendor_Number], [Vendor_Long_Name], [Vendor_Short_Name], [Vendor_Type_Code], [Vendor_Type_Name], [Address_1], [Address_2], [City], [State], [Zip], [Country], [Active], [IndirectDirect], [Status_ID], [Status], [RTV_Credit_Allowed], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'', 0, N'Failed Lookup', N'Failed Lookup', N'', N'', N'', N'', N'', N'', N'', N'', 0, N'', N'', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Vendor] OFF
GO
SET IDENTITY_INSERT [ETL].[AzureMetadataIngestionList] ON 
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (1, N'pcrundata.tbgroup', N'GROUPID, VENDOR', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (2, N'ipcorpdta.scupbrn', N'SCBTYPE, SCBDIV, SCBPC, SCBBRANCH, SCBDATE, SCBHOUR', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/sales/DB203002', 1)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (3, N'ipcorpdta.scupsls', N'SCSTYPE, SCSDIV, SCSPC, SCSBRANCH, SCSDATE, SCSHOUR', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/sales/DB203002', 1)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (4, N'pcrundata.pcpmcrpcst', N'CCTYPE, CCSKU, CCEFFDATE, CCCRTDATE, CCCRTTIME', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (5, N'pcrundata.tbatrgrp', N'GROUPID, ATRID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (6, N'pcrundata.tbatrmlu', N'ATRID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (7, N'pcrundata.tbatrmlv', N'ATRID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (8, N'pcrundata.tbatrrel', N'MAINATRID, RELATRID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (9, N'pcrundata.tbbgtdly', N'DIVISION, PC, BRANCH, BGTDATE', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/sales/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (10, N'pcrundata.tbcid', N'CIDNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:16.787' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (11, N'pcrundata.tbcidadr', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:31.957' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (12, N'pcrundata.tbcideml', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:19.490' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (13, N'pcrundata.tbcidnam', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:21.567' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (14, N'pcrundata.tbcidphn', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:18.603' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (15, N'pcrundata.tbcidprf', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', CAST(N'2019-11-11T13:02:20.787' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (16, N'pcrundata.tbcidtrn', N'CIDNUM, PRFSEQNUM, DIVISION, PC, TRANNUMBER, TRANTYPE', N'UPDATED', N'TIMESTAMP', CAST(N'2019-10-01T00:00:00.000' AS DateTime), N'SELECT * ', N'datalake/staging/master data/customer/DB203002', 1)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (17, N'pcrundata.tbcolnam', N'COLLID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (18, N'pcrundata.tbitem', N'ITEM', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (19, N'pcrundata.tbprm', N'PRMKEY', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/other/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (20, N'pcrundata.tbprmitm', N'PRMKEY, ITMKEY', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (21, N'pcrundata.tbprmloc', N'PRMKEY, LOCKEY', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/other/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (22, N'pcrundata.tbprod', N'GROUPID, PRODID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (23, N'pcrundata.tbprodml', N'PRODID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (24, N'pcrundata.tbpromo', N'PROMO', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/other/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (25, N'pcrundata.tbrptcls', N'CLASS', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (26, N'pcrundata.tbsoadef', N'ATRDEFID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (27, N'pcrundata.tbsoatyp', N'ATRTYPID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (28, N'pcrundata.tbsogdef', N'GRADEID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (29, N'pcrundata.tbsoitmatr', N'ITEM, GROUPID, PRODID, ATRTYPID, GRADEID, ATRDEFID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (30, N'pcrundata.tbsp', N'PROKEY', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/other/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (31, N'pcrundata.tbsploc', N'PROKEY, PROSEQ, DIVPC', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/other/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (32, N'pcrundata.tbusrapp', N'USERID, APPID, ROLEID', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/employee/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (33, N'pcrundata.tbusrdtl', N'USERID  ', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/employee/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (34, N'pcrundata.tbgrpsku', N'GROUPSKU, SKUITEM', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (36, N'pcrundata.tblftitm', N'ITEM', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (37, N'pcrundata.pcpmtblpcn', N'PCCOMPANY, PCDIVISION, PCPC', NULL, NULL, NULL, N'SELECT * ', N'datalake/staging/master data/location/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (38, N'ipcorpdta.tbfph', N'PC, BRANCH, GROUPID, SKU', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (39, N'ipcorpdta.tbfpd', N'PC, BRANCH, GROUPID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (41, N'ipcorpdta.pcpmskustk', N'SKUSTKSKU', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (42, N'pcrundata.tborigin', N'ORIGINID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (43, N'ipcorpdta.ippmvendor', N'VENDORID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/vendor/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (44, N'pcrundata.tbfactry', N'FACTORYID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/factory/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (46, N'pcrundata.tbatrprd', N'PRODID, GROUPID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (48, N'pcrundata.tbitmurl', N'ITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (50, N'ipcorpdta.tbvendor', N'VENDOR', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/vendor/DB203002', 0)
GO
INSERT [ETL].[AzureMetadataIngestionList] ([AzureTableID], [SchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDatatype], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [Disabled]) VALUES (52, N'pcrundata.tbitemstat', N'ITEM,EFFECTIVE_DATE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', 0)
GO
SET IDENTITY_INSERT [ETL].[AzureMetadataIngestionList] OFF
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'CDCOffsetDays', N'3')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AnalysisServicesServer', N'asazure://eastus.asazure.windows.net/dataanalyticsqaaas21')
GO
SET IDENTITY_INSERT [ETL].[DB2MetadataIngestionList] ON 
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (10, N'ipcorpdta.tbdwwrt', N'MARKET,SALENUM,LINE,TYPE,TRANTYPE,TRANSEQUENCE', N'ipcorpdta.tbdwslsctl', N'writtendate', CAST(N'2015-01-01T00:00:00.000' AS DateTime), CAST(N'2016-12-31T00:00:00.000' AS DateTime), NULL, N'Date', 0, N'SELECT * ', N'datalake/staging/sales/DB203002')
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (11, N'ipcorpdta.tbdwbld', N'MARKET,SALENUM,LINE,TYPE,TRANTYPE,TRANSEQUENCE', N'ipcorpdta.tbdwslsctl', N'billeddate', CAST(N'2015-01-01T00:00:00.000' AS DateTime), CAST(N'2016-12-31T00:00:00.000' AS DateTime), NULL, N'Date', 0, N'SELECT * ', N'datalake/staging/sales/DB203002')
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (12, N'ipcorpdta.tbdwloc', N'division, center, branchid', N'ipcorpdta.tbdwlocctl', NULL, CAST(N'2019-11-11T01:00:06.357' AS DateTime), NULL, NULL, NULL, 44, N'SELECT *', N'datalake/staging/master data/location/DB203002')
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (15, N'ipcorpdta.tbdwemp', N'employee_number', N'ipcorpdta.tbdwempctl', NULL, CAST(N'2019-11-11T01:00:05.837' AS DateTime), NULL, NULL, NULL, 18, N'SELECT *', N'datalake/staging/master data/employee/DB203002')
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (16, N'ipcorpdta.tbdwwrtdel', N'MARKET,SALENUM,LINE,TYPE,TRANTYPE,TRANSEQUENCE', N'ipcorpdta.tbdwslsctl', N'writtendate', CAST(N'2019-11-02T00:00:00.000' AS DateTime), CAST(N'2019-11-08T00:00:00.000' AS DateTime), NULL, N'Date', 67, N'SELECT * ', N'datalake/staging/sales/DB203002')
GO
INSERT [ETL].[DB2MetadataIngestionList] ([DB2TableID], [SchemaTable], [PKColumnList], [ControlTable], [ControlFilterColumn], [ControlStartDate], [ControlEndDate], [ControlFilterValue], [ControlFilterType], [ControlProcessNumber], [SelectQuery], [DataLakeStagingFolder]) VALUES (17, N'ipcorpdta.tbdwblddel', N'MARKET,SALENUM,LINE,TYPE,TRANTYPE,TRANSEQUENCE', N'ipcorpdta.tbdwslsctl', N'billeddate', CAST(N'2019-11-02T00:00:00.000' AS DateTime), CAST(N'2019-11-08T00:00:00.000' AS DateTime), NULL, N'Date', 67, N'SELECT * ', N'datalake/staging/sales/DB203002')
GO
SET IDENTITY_INSERT [ETL].[DB2MetadataIngestionList] OFF
GO
SET IDENTITY_INSERT [ETL].[DWLoadList] ON 
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (1, N'DW.Dim_CreditPromotion', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (2, N'DW.Dim_Customer', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (4, N'DW.Dim_Employee', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (5, N'DW.Dim_Factory', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (6, N'DW.Dim_Location', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (7, N'DW.Dim_MerchandisingPromotion', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (8, N'DW.Dim_OverrideReason', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (9, N'DW.Dim_SKU', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (10, N'DW.Dim_SKU_CommonColor', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (11, N'DW.Dim_SKU_CommonFinish', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (12, N'DW.Dim_SKU_Materials', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (13, N'DW.Dim_SKU_SpecialOrderAttributes', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (14, N'DW.Dim_SKU_SpecialOrderAttributesKey', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (15, N'DW.Dim_Team', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (17, N'DW.Dim_Vendor', N'Dim', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (18, N'DW.Fact_CommonColorBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (19, N'DW.Fact_CommonFinishBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (20, N'DW.Fact_DesignerTeamBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (21, N'DW.Fact_MaterialsBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (22, N'DW.Fact_SalesTeamBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (23, N'DW.Fact_SpecialOrderAttributesKeyValuesBridge', N'Bridge', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (24, N'Sales.Fact_DeliveredSales', N'Fact', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (25, N'Sales.Fact_SalesBudget', N'Fact', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (26, N'Sales.Fact_SKUCost', N'Fact', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (27, N'Sales.Fact_Traffic', N'Fact', 0)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled]) VALUES (28, N'Sales.Fact_WrittenSales', N'Fact', 0)
GO
SET IDENTITY_INSERT [ETL].[DWLoadList] OFF
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] ON 
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (1, N'NightlyLoad', N'Azure', 1, N'Pcrundata.TbGroup', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (2, N'NightlyLoad', N'Azure', 2, N'ipcorpdta.scupbrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (3, N'NightlyLoad', N'Azure', 3, N'ipcorpdta.scupsls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (4, N'NightlyLoad', N'Azure', 4, N'pcrundata.pcpmcrpcst', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (5, N'NightlyLoad', N'Azure', 5, N'pcrundata.tbatrgrp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (6, N'NightlyLoad', N'Azure', 6, N'pcrundata.tbatrmlu', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (7, N'NightlyLoad', N'Azure', 7, N'pcrundata.tbatrmlv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (8, N'NightlyLoad', N'Azure', 8, N'pcrundata.tbatrrel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (9, N'NightlyLoad', N'Azure', 9, N'pcrundata.tbbgtdly', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (10, N'NightlyLoad', N'Azure', 10, N'pcrundata.tbcid', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (11, N'NightlyLoad', N'Azure', 11, N'pcrundata.tbcidadr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (12, N'NightlyLoad', N'Azure', 12, N'pcrundata.tbcideml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (13, N'NightlyLoad', N'Azure', 13, N'pcrundata.tbcidnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (14, N'NightlyLoad', N'Azure', 14, N'pcrundata.tbcidphn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (15, N'NightlyLoad', N'Azure', 15, N'pcrundata.tbcidprf', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (16, N'NightlyLoad', N'Azure', 16, N'pcrundata.tbcidtrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (17, N'NightlyLoad', N'Azure', 17, N'pcrundata.tbcolnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (18, N'NightlyLoad', N'Azure', 18, N'pcrundata.tbitem', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (19, N'NightlyLoad', N'Azure', 19, N'pcrundata.tbprm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (20, N'NightlyLoad', N'Azure', 20, N'pcrundata.tbprmitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (21, N'NightlyLoad', N'Azure', 21, N'pcrundata.tbprmloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (22, N'NightlyLoad', N'Azure', 22, N'pcrundata.tbprod', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (23, N'NightlyLoad', N'Azure', 23, N'pcrundata.tbprodml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (24, N'NightlyLoad', N'Azure', 24, N'pcrundata.tbpromo', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (25, N'NightlyLoad', N'Azure', 25, N'pcrundata.tbrptcls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (26, N'NightlyLoad', N'Azure', 26, N'pcrundata.tbsoadef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (27, N'NightlyLoad', N'Azure', 27, N'pcrundata.tbsoatyp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (28, N'NightlyLoad', N'Azure', 28, N'pcrundata.tbsogdef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (29, N'NightlyLoad', N'Azure', 29, N'pcrundata.tbsoitmatr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (30, N'NightlyLoad', N'Azure', 30, N'pcrundata.tbsp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (31, N'NightlyLoad', N'Azure', 31, N'pcrundata.tbsploc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (32, N'NightlyLoad', N'Azure', 32, N'pcrundata.tbusrapp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (33, N'NightlyLoad', N'Azure', 33, N'pcrundata.tbusrdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (34, N'NightlyLoad', N'Azure', 34, N'pcrundata.tbgrpsku', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (35, N'NightlyLoad', N'Azure', 35, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (36, N'NightlyLoad', N'Azure', 36, N'pcrundata.tblftitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (37, N'NightlyLoad', N'Azure', 37, N'pcrundata.pcpmtblpcn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (38, N'NightlyLoad', N'DB2', 16, N'ipcorpdta.tbdwwrtdel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (39, N'NightlyLoad', N'DB2', 17, N'ipcorpdta.tbdwblddel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (40, N'NightlyLoad', N'DB2', 12, N'ipcorpdta.tbdwloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (42, N'NightlyLoad', N'Azure', 38, N'ipcorpdta.tbfph', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (43, N'NightlyLoad', N'Azure', 39, N'ipcorpdta.tbfpd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (45, N'NightlyLoad', N'Azure', 41, N'ipcorpdta.pcpmskustk', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (46, N'NightlyLoad', N'Azure', 42, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (47, N'NightlyLoad', N'Azure', 43, N'ipcorpdta.ippmvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (48, N'NightlyLoad', N'Azure', 44, N'pcrundata.tbfactry', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (49, N'NightlyLoad', N'Azure', 46, N'pcrundata.tbatrprd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (51, N'NightlyLoad', N'Azure', 48, N'pcrundata.tbitmurl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (52, N'NightlyLoad', N'Azure', 50, N'ipcorpdta.tbvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (53, N'NightlyLoad', N'DB2', 15, N'ipcorpdta.tbdwemp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (54, N'OverrideLoad', N'Azure', 1, N'Pcrundata.TbGroup', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (55, N'OverrideLoad', N'Azure', 2, N'ipcorpdta.scupbrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (56, N'OverrideLoad', N'Azure', 3, N'ipcorpdta.scupsls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (57, N'OverrideLoad', N'Azure', 4, N'pcrundata.pcpmcrpcst', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (58, N'OverrideLoad', N'Azure', 5, N'pcrundata.tbatrgrp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (59, N'OverrideLoad', N'Azure', 6, N'pcrundata.tbatrmlu', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (60, N'OverrideLoad', N'Azure', 7, N'pcrundata.tbatrmlv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (61, N'OverrideLoad', N'Azure', 8, N'pcrundata.tbatrrel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (62, N'OverrideLoad', N'Azure', 9, N'pcrundata.tbbgtdly', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (63, N'OverrideLoad', N'Azure', 10, N'pcrundata.tbcid', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (64, N'OverrideLoad', N'Azure', 11, N'pcrundata.tbcidadr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (65, N'OverrideLoad', N'Azure', 12, N'pcrundata.tbcideml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (66, N'OverrideLoad', N'Azure', 13, N'pcrundata.tbcidnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (67, N'OverrideLoad', N'Azure', 14, N'pcrundata.tbcidphn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (68, N'OverrideLoad', N'Azure', 15, N'pcrundata.tbcidprf', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (69, N'OverrideLoad', N'Azure', 16, N'pcrundata.tbcidtrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (70, N'OverrideLoad', N'Azure', 17, N'pcrundata.tbcolnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (71, N'OverrideLoad', N'Azure', 18, N'pcrundata.tbitem', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (72, N'OverrideLoad', N'Azure', 19, N'pcrundata.tbprm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (73, N'OverrideLoad', N'Azure', 20, N'pcrundata.tbprmitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (74, N'OverrideLoad', N'Azure', 21, N'pcrundata.tbprmloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (75, N'OverrideLoad', N'Azure', 22, N'pcrundata.tbprod', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (76, N'OverrideLoad', N'Azure', 23, N'pcrundata.tbprodml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (77, N'OverrideLoad', N'Azure', 24, N'pcrundata.tbpromo', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (78, N'OverrideLoad', N'Azure', 25, N'pcrundata.tbrptcls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (79, N'OverrideLoad', N'Azure', 26, N'pcrundata.tbsoadef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (80, N'OverrideLoad', N'Azure', 27, N'pcrundata.tbsoatyp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (81, N'OverrideLoad', N'Azure', 28, N'pcrundata.tbsogdef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (82, N'OverrideLoad', N'Azure', 29, N'pcrundata.tbsoitmatr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (83, N'OverrideLoad', N'Azure', 30, N'pcrundata.tbsp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (84, N'OverrideLoad', N'Azure', 31, N'pcrundata.tbsploc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (85, N'OverrideLoad', N'Azure', 32, N'pcrundata.tbusrapp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (86, N'OverrideLoad', N'Azure', 33, N'pcrundata.tbusrdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (87, N'OverrideLoad', N'Azure', 34, N'pcrundata.tbgrpsku', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (88, N'OverrideLoad', N'Azure', 35, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (89, N'OverrideLoad', N'Azure', 36, N'pcrundata.tblftitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (90, N'OverrideLoad', N'Azure', 37, N'pcrundata.pcpmtblpcn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (91, N'OverrideLoad', N'DB2', 10, N'ipcorpdta.tbdwwrt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (92, N'OverrideLoad', N'DB2', 11, N'ipcorpdta.tbdwbld', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (93, N'OverrideLoad', N'DB2', 12, N'ipcorpdta.tbdwloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (94, N'OverrideLoad', N'Azure', 38, N'ipcorpdta.tbfph', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (95, N'OverrideLoad', N'Azure', 39, N'ipcorpdta.tbfpd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (96, N'OverrideLoad', N'Azure', 41, N'ipcorpdta.pcpmskustk', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (97, N'OverrideLoad', N'Azure', 42, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (98, N'OverrideLoad', N'Azure', 43, N'ipcorpdta.ippmvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (99, N'OverrideLoad', N'Azure', 44, N'pcrundata.tbfactry', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (100, N'OverrideLoad', N'Azure', 46, N'pcrundata.tbatrprd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (101, N'OverrideLoad', N'Azure', 48, N'pcrundata.tbitmurl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (102, N'OverrideLoad', N'Azure', 50, N'ipcorpdta.tbvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (103, N'OverrideLoad', N'DB2', 15, N'ipcorpdta.tbdwemp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (106, N'NightlyLoad', N'DW', 1, N'DW.Dim_CreditPromotion', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (107, N'NightlyLoad', N'DW', 2, N'DW.Dim_Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (108, N'NightlyLoad', N'DW', 3, N'DW.Dim_Date', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (109, N'NightlyLoad', N'DW', 4, N'DW.Dim_Employee', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (110, N'NightlyLoad', N'DW', 5, N'DW.Dim_Factory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (111, N'NightlyLoad', N'DW', 6, N'DW.Dim_Location', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (112, N'NightlyLoad', N'DW', 7, N'DW.Dim_MerchandisingPromotion', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (113, N'NightlyLoad', N'DW', 8, N'DW.Dim_OverrideReason', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (114, N'NightlyLoad', N'DW', 9, N'DW.Dim_SKU', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (115, N'NightlyLoad', N'DW', 10, N'DW.Dim_SKU_CommonColor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (116, N'NightlyLoad', N'DW', 11, N'DW.Dim_SKU_CommonFinish', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (117, N'NightlyLoad', N'DW', 12, N'DW.Dim_SKU_Materials', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (118, N'NightlyLoad', N'DW', 13, N'DW.Dim_SKU_SpecialOrderAttributes', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (119, N'NightlyLoad', N'DW', 14, N'DW.Dim_SKU_SpecialOrderAttributesKey', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (120, N'NightlyLoad', N'DW', 15, N'DW.Dim_Team', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (121, N'NightlyLoad', N'DW', 16, N'DW.Dim_Time', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (122, N'NightlyLoad', N'DW', 17, N'DW.Dim_Vendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (123, N'NightlyLoad', N'DW', 18, N'DW.Fact_CommonColorBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (124, N'NightlyLoad', N'DW', 19, N'DW.Fact_CommonFinishBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (125, N'NightlyLoad', N'DW', 20, N'DW.Fact_DesignerTeamBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (126, N'NightlyLoad', N'DW', 21, N'DW.Fact_MaterialsBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (127, N'NightlyLoad', N'DW', 22, N'DW.Fact_SalesTeamBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (128, N'NightlyLoad', N'DW', 23, N'DW.Fact_SpecialOrderAttributesKeyValuesBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (129, N'NightlyLoad', N'DW', 24, N'Sales.Fact_DeliveredSales', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (130, N'NightlyLoad', N'DW', 25, N'Sales.Fact_SalesBudget', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (131, N'NightlyLoad', N'DW', 26, N'Sales.Fact_SKUCost', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (132, N'NightlyLoad', N'DW', 27, N'Sales.Fact_Traffic', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (133, N'NightlyLoad', N'DW', 28, N'Sales.Fact_WrittenSales', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (134, N'OverrideLoad', N'DW', 1, N'DW.Dim_CreditPromotion', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (135, N'OverrideLoad', N'DW', 2, N'DW.Dim_Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (136, N'OverrideLoad', N'DW', 3, N'DW.Dim_Date', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (137, N'OverrideLoad', N'DW', 4, N'DW.Dim_Employee', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (138, N'OverrideLoad', N'DW', 5, N'DW.Dim_Factory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (139, N'OverrideLoad', N'DW', 6, N'DW.Dim_Location', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (140, N'OverrideLoad', N'DW', 7, N'DW.Dim_MerchandisingPromotion', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (141, N'OverrideLoad', N'DW', 8, N'DW.Dim_OverrideReason', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (142, N'OverrideLoad', N'DW', 9, N'DW.Dim_SKU', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (143, N'OverrideLoad', N'DW', 10, N'DW.Dim_SKU_CommonColor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (144, N'OverrideLoad', N'DW', 11, N'DW.Dim_SKU_CommonFinish', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (145, N'OverrideLoad', N'DW', 12, N'DW.Dim_SKU_Materials', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (146, N'OverrideLoad', N'DW', 13, N'DW.Dim_SKU_SpecialOrderAttributes', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (147, N'OverrideLoad', N'DW', 14, N'DW.Dim_SKU_SpecialOrderAttributesKey', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (148, N'OverrideLoad', N'DW', 15, N'DW.Dim_Team', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (149, N'OverrideLoad', N'DW', 16, N'DW.Dim_Time', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (150, N'OverrideLoad', N'DW', 17, N'DW.Dim_Vendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (151, N'OverrideLoad', N'DW', 18, N'DW.Fact_CommonColorBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (152, N'OverrideLoad', N'DW', 19, N'DW.Fact_CommonFinishBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (153, N'OverrideLoad', N'DW', 20, N'DW.Fact_DesignerTeamBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (154, N'OverrideLoad', N'DW', 21, N'DW.Fact_MaterialsBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (155, N'OverrideLoad', N'DW', 22, N'DW.Fact_SalesTeamBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (156, N'OverrideLoad', N'DW', 23, N'DW.Fact_SpecialOrderAttributesKeyValuesBridge', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (157, N'OverrideLoad', N'DW', 24, N'Sales.Fact_DeliveredSales', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (158, N'OverrideLoad', N'DW', 25, N'Sales.Fact_SalesBudget', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (159, N'OverrideLoad', N'DW', 26, N'Sales.Fact_SKUCost', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (160, N'OverrideLoad', N'DW', 27, N'Sales.Fact_Traffic', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (161, N'OverrideLoad', N'DW', 28, N'Sales.Fact_WrittenSales', 1)
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] OFF
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'NightlyLoad', 10)
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'OverrideLoad', 10)
