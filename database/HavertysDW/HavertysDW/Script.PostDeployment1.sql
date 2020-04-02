SET IDENTITY_INSERT [DW].[Dim_CreditPromotion] ON 
GO
INSERT [DW].[Dim_CreditPromotion] ([Credit_Promotion_sk], [Credit_Promotion_Code_nk], [Credit_Promotion_Description], [Minimum_Sale_Amount], [Down_Payment_Percentage], [Plan_Name], [Plan_Description], [Duration], [Promo_APR], [Purchase_APR], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, N'-1', N'No Credit Promotion', CAST(0.00 AS Decimal(8, 2)), 0, N'', N'', 0, CAST(0.00 AS Decimal(4, 2)), CAST(0.00 AS Decimal(4, 2)), NULL, 0, 0, 0)
INSERT [DW].[Dim_CreditPromotion] ([Credit_Promotion_sk], [Credit_Promotion_Code_nk], [Credit_Promotion_Description], [Minimum_Sale_Amount], [Down_Payment_Percentage], [Plan_Name], [Plan_Description], [Duration], [Promo_APR], [Purchase_APR], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'0', N'Failed LookUp', CAST(0.00 AS Decimal(8, 2)), 0, N'', N'', 0, CAST(0.00 AS Decimal(4, 2)), CAST(0.00 AS Decimal(4, 2)), NULL, 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_CreditPromotion] OFF 
GO

SET IDENTITY_INSERT [DW].[Dim_Customer] ON 
GO
INSERT [DW].[Dim_Customer] ([Customer_sk], [Customer_Sequence_nk], [Customer_ID_nk], [Sequence_Status], [Bill_First_Name], [Bill_Last_Name], [Bill_Name_Status], [Bill_Address], [Bill_City], [Bill_State], [Bill_ZIP], [Bill_ZIP4], [Bill_Latitude], [Bill_Longitude], [Bill_Address_Status], [Delivery_First_Name], [Delivery_Last_Name], [Delivery_Name_Status], [Delivery_Address], [Delivery_City], [Delivery_State], [Delivery_ZIP], [Delivery_ZIP4], [Delivery_Latitude], [Delivery_Longitude], [Delivery_Address_Status], [Customer_Profile_Created_Date], [Customer_First_Name], [Customer_Last_Name], [Customer_Name_Status], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZIP], [Customer_ZIP4], [Customer_Latitude], [Customer_Longitude], [Customer_Phone_Type], [Customer_Phone], [Customer_Phone_Extension], [Customer_Phone_Status], [Customer_Email], [Customer_Email_Status], [Customer_Created_Date], [Customer_Status], [No_Email], [Room_Plan], [Send_Advertising], [Send_Catalog], [Send_News], [Send_P_Mail], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, 0, N'', N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', CAST(N'1900-01-01' AS Date), N'', N'', N'', N'', N'', N'', N'', N'', CAST(0.000000 AS Decimal(9, 6)), CAST(0.000000 AS Decimal(9, 6)), N'', N'', N'', N'', N'', N'', CAST(N'1900-01-01' AS Date), N'', N'', N'', N'', N'', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Customer] OFF
GO

INSERT [DW].[Dim_Date] ([Date_sk], [Isworkday_Store], [Isworkday_Delivery], [Calendar_date], [Calendar_name_sht], [Calendar_name_lng], [Calendar_year_day_number], [Calendar_year_day_name], [Calendar_quarter_day_number], [Calendar_quarter_day_name], [Calendar_month_day_number], [Calendar_month_day_name], [Calendar_weekday_number], [Calendar_weekday_code], [Calendar_weekday_name], [Calendar_week_of_month_number], [Calendar_week_of_month_name], [Calendar_week_of_year_number], [Calendar_week_of_year_name], [Calendar_weekbegin_date], [Calendar_weekending_date], [Calendar_weekending_name], [Calendar_month_number], [Calendar_month_code], [Calendar_month_name], [Calendar_quarter_number], [Calendar_quarter_code], [Calendar_quarter_name], [Calendar_month_week_year_number], [Calendar_month_week_year_code], [Calendar_month_year_number], [Calendar_month_year_code], [Calendar_quarter_year_number], [Calendar_quarter_year_code], [Calendar_quarter_year_name], [Calendar_year_number], [Calendar_year_name], [Reporting_week_number], [Reporting_weekending_date], [Reporting_weekending_name], [Reporting_weekday_number], [Reporting_month_number], [Reporting_month_code], [Reporting_month_name], [Reporting_quarter_number], [Reporting_quarter_code], [Reporting_quarter_name], [Reporting_month_year_number], [Reporting_month_year_code], [Reporting_quarter_year_number], [Reporting_quarter_year_code], [Reporting_quarter_year_name], [Reporting_year_number], [Reporting_year_name], [Holiday_name], [SameDayOfWeekLastYear_date], [SameDayOfWeekLastYear_fk], [ETLBatchID_Insert], [ETLBatchID_Update], [Reporting_weekbegin_date]) VALUES (0, 0, 0, CAST(N'1900-01-01' AS Date), N'', N'', 0, N'', 0, N'', 0, N'', 0, N'   ', N'', 0, N'', 0, N'', CAST(N'1899-12-31' AS Date), CAST(N'1900-01-06' AS Date), N'WE  Jan 06', 0, N'   ', N'', 0, N'  ', N'', 0, N'', 0, N'', 0, N'', N'', 0, N'', 0, CAST(N'1900-01-01' AS Date), N'', 0, 0, N'   ', N'', 0, N'  ', N'', 0, N'', 0, N'', N'', 0, N'', N'', CAST(N'1899-01-02' AS Date), 18990102, NULL, NULL, NULL)
GO

SET IDENTITY_INSERT [DW].[Dim_DeliveryArea] ON 
GO
INSERT INTO [DW].[Dim_DeliveryArea] ([Delivery_Area_sk], [Delivery_Area_ID_nk], [Delivery_Area_Type_Code], [Delivery_Area_Type], [Delivery_Area_Short_Name], [Delivery_Area_Name], [Delivering_DC_ID], [Delivering_DC], [Drop_Off_Market_ID], [Drop_Off_Market], [Drop_Off_Location_ID], [Drop_Off_Location], [Serivce_Tech_Market_ID], [Serivce_Tech_Market], [Service_Tech_Location_ID], [Service_Tech_Location], [Customer_Pickup_Market_ID], [Customer_Pickup_Market], [Active_Date], [Delivery_Area_Status], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0,0,N'',N'',N'',N'',0,N'',0,N'',0,N'',0,N'',0,N'',0,N'',CAST(N'1900-01-01' AS Date),N'',N'',0,0,0)
GO
SET IDENTITY_INSERT [DW].[Dim_DeliveryArea] OFF 
GO

SET IDENTITY_INSERT [DW].[Dim_Employee] ON
GO 
INSERT [DW].[Dim_Employee] ([Employee_sk], [Employee_ID_nk], [EmployeeType_Code], [EmployeeType_Name], [Sales_ID], [WMS_ID], [Driver_ID], [Designer_ID], [First_Name], [Last_Name], [Full_Name], [Division_ID], [Market_ID], [Branch_ID], [Common_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, N'-1', N'E', N'Employee', 0, 0, 0, -1, N'', N'', N'', 0, 0, 0, N'', N'0                                       ', 0, 0, 0)
INSERT [DW].[Dim_Employee] ([Employee_sk], [Employee_ID_nk], [EmployeeType_Code], [EmployeeType_Name], [Sales_ID], [WMS_ID], [Driver_ID], [Designer_ID], [First_Name], [Last_Name], [Full_Name], [Division_ID], [Market_ID], [Branch_ID], [Common_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'0000000000', N'E', N'Employee', 0, 0, 0, 0, N'Failed Lookup', N'Failed Lookup', N'Failed Lookup', 0, 0, 0, N'', N'0                                       ', 0, 0, 0)
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
INSERT [DW].[Dim_MerchandisingPromotion] ([Merchandising_Promotion_sk], [Merchandising_Promotion_Key_nk], [Merchandising_Promotion_Code_nk], [Merchandising_Promotion_Description], [Type_Code], [Type], [Start_Date], [End_Date], [Segment_Code], [Segment], [Available_on_Internet], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'', N'No Merchandising Promotion', N'', N'', CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, N'', N'', NULL, 0, 0, 0) 
INSERT [DW].[Dim_MerchandisingPromotion] ([Merchandising_Promotion_sk], [Merchandising_Promotion_Key_nk], [Merchandising_Promotion_Code_nk], [Merchandising_Promotion_Description], [Type_Code], [Type], [Start_Date], [End_Date], [Segment_Code], [Segment], [Available_on_Internet], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'', N'', N'', N'', CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, N'', N'', NULL, 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_MerchandisingPromotion] OFF
GO

SET IDENTITY_INSERT [DW].[Dim_OverrideReason] ON 
GO
INSERT [DW].[Dim_OverrideReason] ([Override_Reason_sk], [Override_Reason_ID], [Override_Reason], [Override_Reason_Text], [Override_Approve_First_Name], [Override_Approve_Last_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'No Override', N'No Override', N'', N'', N'                                        ', 0, 0, 0)
INSERT [DW].[Dim_OverrideReason] ([Override_Reason_sk], [Override_Reason_ID], [Override_Reason], [Override_Reason_Text], [Override_Approve_First_Name], [Override_Approve_Last_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'Failed Lookup', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_OverrideReason] OFF

GO
SET IDENTITY_INSERT [DW].[Dim_ShippingOrigin] ON 
GO
INSERT [DW].[Dim_ShippingOrigin] ([Shipping_Origin_sk], [Shipping_Origin_ID_nk], [Shipping_Origin_Code], [Shipping_Origin_UN_Location_Code], [Shipping_Origin_Name], [Shipping_Origin_Type_Code], [Shipping_Origin_Type], [City], [State_Code], [State], [Country_Code], [Country], [Latitude], [Longitude], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (2, 0, N'', N'', N'Failed Lookup', N'', N'', N'Failed Lookup', N'', N'Failed Lookup', N'', N'Failed Lookup', CAST(0.000000 AS Numeric(9, 6)), CAST(0.000000 AS Numeric(9, 6)), N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_ShippingOrigin] OFF

SET IDENTITY_INSERT [DW].[Dim_SKU] ON 
GO
INSERT [DW].[Dim_SKU] ([SKU_sk], [SKU_Unformatted_nk], [SKU], [SKU_Description], [Vendor_Color_ID], [Vendor_Color], [Vendor_Color_Hex_Code], [Vendor_Finish_ID], [Vendor_Finish], [Configuration_ID], [Configuration], [Display_Configuration], [Size_ID], [Size], [Original_Vendor_ID], [Original_Vendor_fk], [Collection_ID], [Collection], [Group_ID], [Group], [Product_ID], [Product], [Reporting_Class_Code], [Reporting_Class], [Super_Category_ID], [Super_Category], [Category_ID], [Category], [Sub_Category_ID], [Sub_Category], [Current_EDLP_Price], [Good_Better_Best_Category], [GBB_Price_Point], [Motion], [Dining_Style], [Bedroom_Style], [Standalone_bed], [SKU_Type], [Stocked], [Type_of_SKU_Code], [Type_of_SKU], [Vendor_ID], [Vendor_fk], [Factory_ID], [Factory_fk], [Special_Order_Available], [Vendor_Lead_Time], [Product_Lead_Time], [Group_Active_Start], [Group_Active_End], [SKU_Flow_StartDate], [SKU_Flow_EndDate], [Stocked_at_EDC], [Stocked_at_WDC], [Stocked_at_FDC], [Group_Web_Status], [SKU_Web_Status], [Merchandising_Approval], [Marketing_Approval], [Lineup_Status_Code], [Lineup_Status], [Available_to_Purchase], [Ship_Length], [Ship_Width], [Ship_Height], [Length], [Width], [Height], [Weight], [Prep_Cubes], [Cubes], [Prep_Flag], [Assembly_Required], [Assembly_Time], [Leave_in_Box], [Manufacturer_ID], [UPC], [HTS_Number], [Original_SKU], [Representative_Item], [Group_Rating_Count], [Product_Rating_Count], [Group_Star_Rating], [Product_Star_Rating], [Made_in_ID], [Made_in_City], [Made_in_State], [Made_in_Country], [Origin_ID], [Origin_City], [Origin_State], [Origin_Country], [Copy], [Web_Description], [Price_Tag_Description], [Primary_Common_Color], [Primary_Common_Finish], [Floor_Sample_Bedding], [Employee_Bedding], [Accessory_Story], [IndirectDirect], [3D_Model_Required], [3D_Model], [Scene_7_Image_URL], [Havertys.com_URL], [Advertised_Price], [Supply_Chain_Analyst_fk], [Supply_Chain_Analyst_nk], [Supply_Chain_Analyst_FullName], [Product_Features], [Origin], [FOB_Cost], [Warehouse_Cost], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'000000000', N'0-0000-0000', N'Failed Lookup', 0, N'', N'', 0, N'', 0, N'', N'', 0, N'', N'', 0, 0, N'', 0, N'', 0, N'', N'', N'', 0, N'', 0, N'', 0, N'', CAST(0.0000 AS Decimal(10, 4)), N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', 0, 0, 0, N'', 0, 0, CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), CAST(N'1900-01-01' AS Date), 0, 0, 0, 0, 0, N'', N'', N'', N'', N'', 0, 0, 0, 0, 0, 0, CAST(0.0 AS Decimal(5, 1)), CAST(0.0 AS Decimal(5, 1)), CAST(0.0 AS Decimal(5, 1)), N'', N'', CAST(0.0 AS Decimal(4, 1)), N'', N'', 0, 0, N'', N'', 0, 0, CAST(0.0000 AS Decimal(6, 4)), CAST(0.0000 AS Decimal(6, 4)), 0, N'', N'', N'', 0, N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', N'', NULL, N'', N'', N'', N'', CAST(0.00 AS Decimal(7, 2)),0,0,N'',N'',N'',CAST(0.00 AS DECIMAL(10, 4)),CAST(0.00 AS DECIMAL(10,4)), N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU] OFF

SET IDENTITY_INSERT [DW].[Dim_SKU_CommonColor] ON 
GO
INSERT [DW].[Dim_SKU_CommonColor] ([Common_Color_sk], [Common_Color_Code_nk], [Common_Color_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'No Common Color', N'                                        ', 0, 0, 0)
INSERT [DW].[Dim_SKU_CommonColor] ([Common_Color_sk], [Common_Color_Code_nk], [Common_Color_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_CommonColor] OFF 
GO

SET IDENTITY_INSERT [DW].[Dim_SKU_CommonFinish] ON 
GO
INSERT [DW].[Dim_SKU_CommonFinish] ([Common_Finish_sk], [Common_Finish_Code_nk], [Common_Finish_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'No Common Finish', N'                                        ', 0, 0, 0)
INSERT [DW].[Dim_SKU_CommonFinish] ([Common_Finish_sk], [Common_Finish_Code_nk], [Common_Finish_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_CommonFinish] OFF
GO

SET IDENTITY_INSERT [DW].[Dim_SKU_Materials] ON 
GO
INSERT [DW].[Dim_SKU_Materials] ([Materials_sk], [Material_Code_nk], [Material_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (-1, -1, N'No Material', N'                                        ', 0, 0, 0)
INSERT [DW].[Dim_SKU_Materials] ([Materials_sk], [Material_Code_nk], [Material_Name], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, 0, N'Failed Lookup', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_SKU_Materials] OFF 
GO

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


SET IDENTITY_INSERT [DW].[Dim_Time] ON 
GO
INSERT [DW].[Dim_Time] ([Time_sk], [Time24_code], [Time24_name], [Hour_code], [Hour_name], [Hour24_code], [Hour24_name], [Minute_code], [Minute_name], [Minute24_code], [Minute24_name], [Second_code], [Second_name], [Second24_code], [Second24_name], [AmPm], [StandardTime], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, NULL, N'', 0, N'', 0, N'', 0, N'', 0, N'', 0, N'', 0, N'', N'', N'', 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Time] OFF

SET IDENTITY_INSERT [DW].[Dim_Vendor] ON 
GO
INSERT [DW].[Dim_Vendor] ([Vendor_sk], [Vendor_ID_nk], [Vendor_Number], [Vendor_Long_Name], [Vendor_Short_Name], [Vendor_Type_Code], [Vendor_Type_Name], [Address_1], [Address_2], [City], [State], [Zip], [Country], [Active], [IndirectDirect], [Status_ID], [Status], [RTV_Credit_Allowed], [Parent_Company], [HashKey], [SourceSystem_fk], [ETLBatchID_Insert], [ETLBatchID_Update]) VALUES (0, N'', 0, N'Failed Lookup', N'Failed Lookup', N'', N'', N'', N'', N'', N'', N'', N'', 0, N'', N'', N'', N'', N'', N'                                        ', 0, 0, 0)
GO
SET IDENTITY_INSERT [DW].[Dim_Vendor] OFF

SET IDENTITY_INSERT [ETL].[BlobFileMetadataIngestionList] ON 
GO
INSERT [ETL].[BlobFileMetadataIngestionList] ([BlobFileID], [BlobFolder], [FileName], [BlobFileType], [DataLakeStagingFolder], [Description]) VALUES (1, N'sharepoint', N'Haverty_Directory', N'json', N'datalake/staging/master data/location/sharepoint', N'Most recent branch manager names from SharePoint directory')
GO
INSERT [ETL].[BlobFileMetadataIngestionList] ([BlobFileID], [BlobFolder], [FileName], [BlobFileType], [DataLakeStagingFolder], [Description]) VALUES (2, N'csv', N'SaleEventsCSV.csv', N'csv', N'datalake/staging/master data/date', N'holidays')
GO
INSERT [ETL].[BlobFileMetadataIngestionList] ([BlobFileID], [BlobFolder], [FileName], [BlobFileType], [DataLakeStagingFolder], [Description]) VALUES (4, N'csv', N'SaleEventsCSV.csv', N'csv', N'datalake/staging/master data/date', N'holidays')
GO
SET IDENTITY_INSERT [ETL].[BlobFileMetadataIngestionList] OFF
GO

INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'CDCOffsetDays', N'3')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AnalysisServicesServer', N'asazure://eastus.asazure.windows.net/dataanalyticsqaaas21')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLServerName', N'dataanalyticsqa-sql21')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLDatabaseName', N'HavertysDW_QA')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SubscriptionID', N'e2f53281-fd58-437e-82e1-088d1b79fcbc')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'ResourceGroupName', N'DataAnalytics_QA_RG')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookFull', N'https://s16events.azure-automation.net/webhooks?token=F2xAYLKuX%2fs1VY%2byw7QFUIL0cQal1QKrBmrISNEKBBU%3d')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookIncremental', N'https://s16events.azure-automation.net/webhooks?token=t7GUWw%2bHL81EfJlHJhqaFSfrB6dkjELv0yWdTjTx7TI%3d')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SQLComputeSizeHigh', N'GP_Gen5_16')
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SQLComputeSizeLow', N'GP_Gen5_2')
GO

SET IDENTITY_INSERT [ETL].[DWLoadList] ON 
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (1, N'DW.Dim_CreditPromotion', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (2, N'DW.Dim_Customer', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (4, N'DW.Dim_Employee', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (5, N'DW.Dim_Factory', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (6, N'DW.Dim_Location', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (7, N'DW.Dim_MerchandisingPromotion', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (8, N'DW.Dim_OverrideReason', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (9, N'DW.Dim_SKU', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (10, N'DW.Dim_SKU_CommonColor', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (11, N'DW.Dim_SKU_CommonFinish', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (12, N'DW.Dim_SKU_Materials', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (13, N'DW.Dim_SKU_SpecialOrderAttributes', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (14, N'DW.Dim_SKU_SpecialOrderAttributesKey', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (15, N'DW.Dim_Team', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (17, N'DW.Dim_Vendor', N'Dim', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (18, N'DW.Fact_CommonColorBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (19, N'DW.Fact_CommonFinishBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (20, N'DW.Fact_DesignerTeamBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (21, N'DW.Fact_MaterialsBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (22, N'DW.Fact_SalesTeamBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (23, N'DW.Fact_SpecialOrderAttributesKeyValuesBridge', N'Bridge', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (24, N'Sales.Fact_DeliveredSales', N'Fact', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (25, N'Sales.Fact_SalesBudget', N'Fact', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (26, N'Sales.Fact_SKUCost', N'Fact', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (27, N'Sales.Fact_Traffic', N'Fact', 0, NULL)
GO
INSERT [ETL].[DWLoadList] ([TableID], [SchemaTable], [DWType], [Disabled], [DbxNotebookPath]) VALUES (28, N'Sales.Fact_WrittenSales', N'Fact', 0, NULL)
GO
SET IDENTITY_INSERT [ETL].[DWLoadList] OFF
GO

SET IDENTITY_INSERT [ETL].[JobConfiguration] ON 
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
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (164, N'NightlyLoad', N'Blob', 1, N'Haverty_Directory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (165, N'OverrideLoad', N'Blob', 1, N'Haverty_Directory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (176, N'OverrideLoad', N'Blob', 2, N'SaleEventsCSV.csv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (182, N'NightlyLoad', N'Blob', 2, N'SaleEventsCSV.csv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (218, N'NightlyLoad', N'DW', 29, N'DW.Dim_ShippingOrigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (219, N'OverrideLoad', N'DW', 29, N'DW.Dim_ShippingOrigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (244, N'NightlyLoad', N'DB2', 1, N'pcrundata.tbgroup', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (245, N'NightlyLoad', N'DB2', 2, N'ipcorpdta.scupbrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (246, N'NightlyLoad', N'DB2', 3, N'ipcorpdta.scupsls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (247, N'NightlyLoad', N'DB2', 4, N'pcrundata.pcpmcrpcst', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (248, N'NightlyLoad', N'DB2', 5, N'pcrundata.tbatrgrp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (249, N'NightlyLoad', N'DB2', 6, N'pcrundata.tbatrmlu', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (250, N'NightlyLoad', N'DB2', 7, N'pcrundata.tbatrmlv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (251, N'NightlyLoad', N'DB2', 8, N'pcrundata.tbatrrel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (252, N'NightlyLoad', N'DB2', 9, N'pcrundata.tbbgtdly', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (253, N'NightlyLoad', N'DB2', 10, N'pcrundata.tbcid', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (254, N'NightlyLoad', N'DB2', 11, N'pcrundata.tbcidadr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (255, N'NightlyLoad', N'DB2', 12, N'pcrundata.tbcideml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (256, N'NightlyLoad', N'DB2', 13, N'pcrundata.tbcidnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (257, N'NightlyLoad', N'DB2', 14, N'pcrundata.tbcidphn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (258, N'NightlyLoad', N'DB2', 15, N'pcrundata.tbcidprf', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (259, N'NightlyLoad', N'DB2', 16, N'pcrundata.tbcidtrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (260, N'NightlyLoad', N'DB2', 17, N'pcrundata.tbcolnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (261, N'NightlyLoad', N'DB2', 18, N'pcrundata.tbitem', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (262, N'NightlyLoad', N'DB2', 19, N'pcrundata.tbprm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (263, N'NightlyLoad', N'DB2', 20, N'pcrundata.tbprmitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (264, N'NightlyLoad', N'DB2', 21, N'pcrundata.tbprmloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (265, N'NightlyLoad', N'DB2', 22, N'pcrundata.tbprod', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (266, N'NightlyLoad', N'DB2', 23, N'pcrundata.tbprodml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (267, N'NightlyLoad', N'DB2', 24, N'pcrundata.tbpromo', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (268, N'NightlyLoad', N'DB2', 25, N'pcrundata.tbrptcls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (269, N'NightlyLoad', N'DB2', 26, N'pcrundata.tbsoadef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (270, N'NightlyLoad', N'DB2', 27, N'pcrundata.tbsoatyp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (271, N'NightlyLoad', N'DB2', 28, N'pcrundata.tbsogdef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (272, N'NightlyLoad', N'DB2', 29, N'pcrundata.tbsoitmatr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (273, N'NightlyLoad', N'DB2', 30, N'pcrundata.tbsp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (274, N'NightlyLoad', N'DB2', 31, N'pcrundata.tbsploc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (275, N'NightlyLoad', N'DB2', 32, N'pcrundata.tbusrapp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (276, N'NightlyLoad', N'DB2', 33, N'pcrundata.tbusrdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (277, N'NightlyLoad', N'DB2', 34, N'pcrundata.tbgrpsku', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (278, N'NightlyLoad', N'DB2', 35, N'pcrundata.tblftitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (279, N'NightlyLoad', N'DB2', 36, N'pcrundata.pcpmtblpcn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (280, N'NightlyLoad', N'DB2', 37, N'ipcorpdta.tbfph', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (281, N'NightlyLoad', N'DB2', 38, N'ipcorpdta.tbfpd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (282, N'NightlyLoad', N'DB2', 39, N'ipcorpdta.pcpmskustk', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (283, N'NightlyLoad', N'DB2', 40, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (284, N'NightlyLoad', N'DB2', 41, N'ipcorpdta.ippmvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (285, N'NightlyLoad', N'DB2', 42, N'pcrundata.tbfactry', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (286, N'NightlyLoad', N'DB2', 43, N'pcrundata.tbatrprd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (287, N'NightlyLoad', N'DB2', 44, N'pcrundata.tbitmurl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (288, N'NightlyLoad', N'DB2', 45, N'ipcorpdta.tbvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (289, N'NightlyLoad', N'DB2', 46, N'pcrundata.tbitemstat', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (290, N'NightlyLoad', N'DB2', 47, N'pcrundata.tbusrnum', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (291, N'NightlyLoad', N'DB2', 48, N'pcrundata.tbsca', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (292, N'NightlyLoad', N'DB2', 49, N'pcrundata.pcpmtblda', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (295, N'NightlyLoad', N'DB2', 52, N'ipcorpdta.tbdwloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (296, N'NightlyLoad', N'DB2', 53, N'ipcorpdta.tbdwemp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (297, N'NightlyLoad', N'DB2', 54, N'ipcorpdta.tbdwwrtdel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (298, N'NightlyLoad', N'DB2', 55, N'ipcorpdta.tbdwblddel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (299, N'NightlyLoad', N'DB2', 56, N'pcrundata.tbdwpohdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (300, N'NightlyLoad', N'DB2', 57, N'pcrundata.tbdwpodtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (301, N'NightlyLoad', N'DB2', 58, N'pcrundata.tbdwdckhdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (302, N'NightlyLoad', N'DB2', 59, N'pcrundata.tbdwdckdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (303, N'NightlyLoad', N'DB2', 60, N'pcrundata.tbdwpol', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (304, N'NightlyLoad', N'DB2', 61, N'pcrundata.tbdwpol000', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (305, N'NightlyLoad', N'DB2', 62, N'pcrundata.tbdwpol001', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (306, N'NightlyLoad', N'DB2', 63, N'pcrundata.tbdwpol002', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (307, N'NightlyLoad', N'DB2', 64, N'pcrundata.tbdwpol003', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (308, N'NightlyLoad', N'DB2', 65, N'pcrundata.tbdwpol004', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (309, N'NightlyLoad', N'DB2', 66, N'pcrundata.tbdwpol005', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (310, N'NightlyLoad', N'DB2', 67, N'pcrundata.tbdwsl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (311, N'NightlyLoad', N'DB2', 68, N'pcrundata.tbdwsl001', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (312, N'NightlyLoad', N'DB2', 69, N'pcrundata.tbdwsl002', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (313, N'NightlyLoad', N'DB2', 70, N'pcrundata.tbdwsl003', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (314, N'NightlyLoad', N'DB2', 71, N'pcrundata.tbdwsl004', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (315, N'NightlyLoad', N'DB2', 72, N'pcrundata.tbdwsl005', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (316, N'NightlyLoad', N'DB2', 73, N'pcrundata.tbshp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (317, N'NightlyLoad', N'DB2', 74, N'nohalib.tbiaivt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (318, N'NightlyLoad', N'DB2', 75, N'pcrundata.pcpmtblzip', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (319, N'NightlyLoad', N'DB2', 76, N'pcrundata.tbsa', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (320, N'NightlyLoad', N'DB2', 77, N'pcrundata.tbdwdsthdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (321, N'NightlyLoad', N'DB2', 78, N'pcrundata.tbdwdstdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (322, N'NightlyLoad', N'DB2', 79, N'pcrundata.tbdwasg', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (323, N'NightlyLoad', N'DB2', 80, N'pcrundata.tbdwsch', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (324, N'NightlyLoad', N'DB2', 81, N'pcrundata.tbdwrte', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (325, N'NightlyLoad', N'DB2', 82, N'pcrundata.tbdwstp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (326, N'NightlyLoad', N'DB2', 83, N'pcrundata.tbdwpochdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (327, N'NightlyLoad', N'DB2', 84, N'pcrundata.tbdwpocdt1', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (328, N'NightlyLoad', N'DB2', 85, N'pcrundata.tbdwpocdt2', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (329, N'NightlyLoad', N'DB2', 86, N'pcrundata.tbdwfif', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (330, N'NightlyLoad', N'DB2', 87, N'pcrundata.tbdwfifdel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (331, N'NightlyLoad', N'DB2', 88, N'pcrundata.tbdwicshdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (332, N'NightlyLoad', N'DB2', 89, N'pcrundata.tbdwicsdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (333, N'NightlyLoad', N'DB2', 90, N'pcrundata.tbdwicsahd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (334, N'NightlyLoad', N'DB2', 91, N'pcrundata.tbdwicsadt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (335, N'NightlyLoad', N'DB2', 92, N'pcrundata.tbdwmnfhdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (336, N'NightlyLoad', N'DB2', 93, N'pcrundata.tbdwmnfdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (337, N'OverrideLoad', N'DB2', 1, N'pcrundata.tbgroup', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (338, N'OverrideLoad', N'DB2', 2, N'ipcorpdta.scupbrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (339, N'OverrideLoad', N'DB2', 3, N'ipcorpdta.scupsls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (340, N'OverrideLoad', N'DB2', 4, N'pcrundata.pcpmcrpcst', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (341, N'OverrideLoad', N'DB2', 5, N'pcrundata.tbatrgrp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (342, N'OverrideLoad', N'DB2', 6, N'pcrundata.tbatrmlu', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (343, N'OverrideLoad', N'DB2', 7, N'pcrundata.tbatrmlv', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (344, N'OverrideLoad', N'DB2', 8, N'pcrundata.tbatrrel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (345, N'OverrideLoad', N'DB2', 9, N'pcrundata.tbbgtdly', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (346, N'OverrideLoad', N'DB2', 10, N'pcrundata.tbcid', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (347, N'OverrideLoad', N'DB2', 11, N'pcrundata.tbcidadr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (348, N'OverrideLoad', N'DB2', 12, N'pcrundata.tbcideml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (349, N'OverrideLoad', N'DB2', 13, N'pcrundata.tbcidnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (350, N'OverrideLoad', N'DB2', 14, N'pcrundata.tbcidphn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (351, N'OverrideLoad', N'DB2', 15, N'pcrundata.tbcidprf', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (352, N'OverrideLoad', N'DB2', 16, N'pcrundata.tbcidtrn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (353, N'OverrideLoad', N'DB2', 17, N'pcrundata.tbcolnam', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (354, N'OverrideLoad', N'DB2', 18, N'pcrundata.tbitem', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (355, N'OverrideLoad', N'DB2', 19, N'pcrundata.tbprm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (356, N'OverrideLoad', N'DB2', 20, N'pcrundata.tbprmitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (357, N'OverrideLoad', N'DB2', 21, N'pcrundata.tbprmloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (358, N'OverrideLoad', N'DB2', 22, N'pcrundata.tbprod', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (359, N'OverrideLoad', N'DB2', 23, N'pcrundata.tbprodml', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (360, N'OverrideLoad', N'DB2', 24, N'pcrundata.tbpromo', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (361, N'OverrideLoad', N'DB2', 25, N'pcrundata.tbrptcls', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (362, N'OverrideLoad', N'DB2', 26, N'pcrundata.tbsoadef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (363, N'OverrideLoad', N'DB2', 27, N'pcrundata.tbsoatyp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (364, N'OverrideLoad', N'DB2', 28, N'pcrundata.tbsogdef', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (365, N'OverrideLoad', N'DB2', 29, N'pcrundata.tbsoitmatr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (366, N'OverrideLoad', N'DB2', 30, N'pcrundata.tbsp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (367, N'OverrideLoad', N'DB2', 31, N'pcrundata.tbsploc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (368, N'OverrideLoad', N'DB2', 32, N'pcrundata.tbusrapp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (369, N'OverrideLoad', N'DB2', 33, N'pcrundata.tbusrdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (370, N'OverrideLoad', N'DB2', 34, N'pcrundata.tbgrpsku', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (371, N'OverrideLoad', N'DB2', 35, N'pcrundata.tblftitm', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (372, N'OverrideLoad', N'DB2', 36, N'pcrundata.pcpmtblpcn', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (373, N'OverrideLoad', N'DB2', 37, N'ipcorpdta.tbfph', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (374, N'OverrideLoad', N'DB2', 38, N'ipcorpdta.tbfpd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (375, N'OverrideLoad', N'DB2', 39, N'ipcorpdta.pcpmskustk', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (376, N'OverrideLoad', N'DB2', 40, N'pcrundata.tborigin', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (377, N'OverrideLoad', N'DB2', 41, N'ipcorpdta.ippmvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (378, N'OverrideLoad', N'DB2', 42, N'pcrundata.tbfactry', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (379, N'OverrideLoad', N'DB2', 43, N'pcrundata.tbatrprd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (380, N'OverrideLoad', N'DB2', 44, N'pcrundata.tbitmurl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (381, N'OverrideLoad', N'DB2', 45, N'ipcorpdta.tbvendor', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (382, N'OverrideLoad', N'DB2', 46, N'pcrundata.tbitemstat', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (383, N'OverrideLoad', N'DB2', 47, N'pcrundata.tbusrnum', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (384, N'OverrideLoad', N'DB2', 48, N'pcrundata.tbsca', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (385, N'OverrideLoad', N'DB2', 49, N'pcrundata.pcpmtblda', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (386, N'OverrideLoad', N'DB2', 50, N'ipcorpdta.tbdwwrt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (387, N'OverrideLoad', N'DB2', 51, N'ipcorpdta.tbdwbld', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (388, N'OverrideLoad', N'DB2', 52, N'ipcorpdta.tbdwloc', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (389, N'OverrideLoad', N'DB2', 53, N'ipcorpdta.tbdwemp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (392, N'OverrideLoad', N'DB2', 56, N'pcrundata.tbdwpohdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (393, N'OverrideLoad', N'DB2', 57, N'pcrundata.tbdwpodtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (394, N'OverrideLoad', N'DB2', 58, N'pcrundata.tbdwdckhdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (395, N'OverrideLoad', N'DB2', 59, N'pcrundata.tbdwdckdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (396, N'OverrideLoad', N'DB2', 60, N'pcrundata.tbdwpol', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (397, N'OverrideLoad', N'DB2', 61, N'pcrundata.tbdwpol000', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (398, N'OverrideLoad', N'DB2', 62, N'pcrundata.tbdwpol001', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (399, N'OverrideLoad', N'DB2', 63, N'pcrundata.tbdwpol002', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (400, N'OverrideLoad', N'DB2', 64, N'pcrundata.tbdwpol003', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (401, N'OverrideLoad', N'DB2', 65, N'pcrundata.tbdwpol004', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (402, N'OverrideLoad', N'DB2', 66, N'pcrundata.tbdwpol005', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (403, N'OverrideLoad', N'DB2', 67, N'pcrundata.tbdwsl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (404, N'OverrideLoad', N'DB2', 68, N'pcrundata.tbdwsl001', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (405, N'OverrideLoad', N'DB2', 69, N'pcrundata.tbdwsl002', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (406, N'OverrideLoad', N'DB2', 70, N'pcrundata.tbdwsl003', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (407, N'OverrideLoad', N'DB2', 71, N'pcrundata.tbdwsl004', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (408, N'OverrideLoad', N'DB2', 72, N'pcrundata.tbdwsl005', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (409, N'OverrideLoad', N'DB2', 73, N'pcrundata.tbshp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (410, N'OverrideLoad', N'DB2', 74, N'nohalib.tbiaivt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (411, N'OverrideLoad', N'DB2', 75, N'pcrundata.pcpmtblzip', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (412, N'OverrideLoad', N'DB2', 76, N'pcrundata.tbsa', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (413, N'OverrideLoad', N'DB2', 77, N'pcrundata.tbdwdsthdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (414, N'OverrideLoad', N'DB2', 78, N'pcrundata.tbdwdstdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (415, N'OverrideLoad', N'DB2', 79, N'pcrundata.tbdwasg', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (416, N'OverrideLoad', N'DB2', 80, N'pcrundata.tbdwsch', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (417, N'OverrideLoad', N'DB2', 81, N'pcrundata.tbdwrte', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (418, N'OverrideLoad', N'DB2', 82, N'pcrundata.tbdwstp', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (419, N'OverrideLoad', N'DB2', 83, N'pcrundata.tbdwpochdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (420, N'OverrideLoad', N'DB2', 84, N'pcrundata.tbdwpocdt1', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (421, N'OverrideLoad', N'DB2', 85, N'pcrundata.tbdwpocdt2', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (422, N'OverrideLoad', N'DB2', 86, N'pcrundata.tbdwfif', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (423, N'OverrideLoad', N'DB2', 87, N'pcrundata.tbdwfifdel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (424, N'OverrideLoad', N'DB2', 88, N'pcrundata.tbdwicshdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (425, N'OverrideLoad', N'DB2', 89, N'pcrundata.tbdwicsdtl', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (426, N'OverrideLoad', N'DB2', 90, N'pcrundata.tbdwicsahd', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (427, N'OverrideLoad', N'DB2', 91, N'pcrundata.tbdwicsadt', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (428, N'OverrideLoad', N'DB2', 92, N'pcrundata.tbdwmnfhdr', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (429, N'OverrideLoad', N'DB2', 93, N'pcrundata.tbdwmnfdtl', 1)
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] OFF
GO

INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'InventoryNightly', N'Run the nightly inventory refresh.')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'InventoryOverride', N'Run the historical inventory refresh.')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'NightlyLoad', N'Runs every night and performs entire ETL process')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'OverrideLoad', N'Runs on-demand and performs entire ETL process')
GO

INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'NightlyLoad', 10)
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'OverrideLoad', 10)
GO

SET IDENTITY_INSERT [ETL].[MetadataIngestionList] ON 
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (1, N'pcrundata.tbgroup', N'pcrundata.tbgroup', N'GROUPID, VENDOR', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (2, N'ipcorpdta.scupbrn', N'ipcorpdta.scupbrn', N'SCBTYPE, SCBDIV, SCBPC, SCBBRANCH, SCBDATE, SCBHOUR', N'SCBDATE', N'INT', N'20200325', N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (3, N'ipcorpdta.scupsls', N'ipcorpdta.scupsls', N'SCSTYPE, SCSDIV, SCSPC, SCSBRANCH, SCSDATE, SCSHOUR', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 1)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (4, N'pcrundata.pcpmcrpcst', N'pcrundata.pcpmcrpcst', N'CCTYPE, CCSKU, CCEFFDATE, CCCRTDATE, CCCRTTIME', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (5, N'pcrundata.tbatrgrp', N'pcrundata.tbatrgrp', N'GROUPID, ATRID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (6, N'pcrundata.tbatrmlu', N'pcrundata.tbatrmlu', N'ATRID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (7, N'pcrundata.tbatrmlv', N'pcrundata.tbatrmlv', N'ATRID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (8, N'pcrundata.tbatrrel', N'pcrundata.tbatrrel', N'MAINATRID, RELATRID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (9, N'pcrundata.tbbgtdly', N'pcrundata.tbbgtdly', N'DIVISION, PC, BRANCH, BGTDATE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (10, N'pcrundata.tbcid', N'pcrundata.tbcid', N'CIDNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-25 14:13:36.453', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (11, N'pcrundata.tbcidadr', N'pcrundata.tbcidadr', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-25 14:21:35.003', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (12, N'pcrundata.tbcideml', N'pcrundata.tbcideml', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-25 14:24:32.680', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (13, N'pcrundata.tbcidnam', N'pcrundata.tbcidnam', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-25 14:26:44.553', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (14, N'pcrundata.tbcidphn', N'pcrundata.tbcidphn', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-25 14:29:06.803', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (15, N'pcrundata.tbcidprf', N'pcrundata.tbcidprf', N'CIDNUM, SEQNUM', N'UPDATED', N'TIMESTAMP', N'2020-03-20 13:02:34.297', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (16, N'pcrundata.tbcidtrn', N'pcrundata.tbcidtrn', N'CIDNUM, PRFSEQNUM, DIVISION, PC, TRANNUMBER, TRANTYPE', N'UPDATED', N'TIMESTAMP', N'2020-03-05 19:39:29.470', N'SELECT *', N'datalake/staging/master data/customer/DB203002', N'PC03002', NULL, 0, 1)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (17, N'pcrundata.tbcolnam', N'pcrundata.tbcolnam', N'COLLID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (18, N'pcrundata.tbitem', N'pcrundata.tbitem', N'ITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (19, N'pcrundata.tbprm', N'pcrundata.tbprm', N'PRMKEY', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (20, N'pcrundata.tbprmitm', N'pcrundata.tbprmitm', N'PRMKEY, ITMKEY', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (21, N'pcrundata.tbprmloc', N'pcrundata.tbprmloc', N'PRMKEY, LOCKEY', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (22, N'pcrundata.tbprod', N'pcrundata.tbprod', N'GROUPID, PRODID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (23, N'pcrundata.tbprodml', N'pcrundata.tbprodml', N'PRODID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (24, N'pcrundata.tbpromo', N'pcrundata.tbpromo', N'PROMO', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (25, N'pcrundata.tbrptcls', N'pcrundata.tbrptcls', N'CLASS', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (26, N'pcrundata.tbsoadef', N'pcrundata.tbsoadef', N'ATRDEFID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (27, N'pcrundata.tbsoatyp', N'pcrundata.tbsoatyp', N'ATRTYPID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (28, N'pcrundata.tbsogdef', N'pcrundata.tbsogdef', N'GRADEID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (29, N'pcrundata.tbsoitmatr', N'pcrundata.tbsoitmatr', N'ITEM, GROUPID, PRODID, ATRTYPID, GRADEID, ATRDEFID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (30, N'pcrundata.tbsp', N'pcrundata.tbsp', N'PROKEY', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (31, N'pcrundata.tbsploc', N'pcrundata.tbsploc', N'PROKEY, PROSEQ, DIVPC', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (32, N'pcrundata.tbusrapp', N'pcrundata.tbusrapp', N'USERID, APPID, ROLEID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/employee/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (33, N'pcrundata.tbusrdtl', N'pcrundata.tbusrdtl', N'USERID  ', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/employee/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (34, N'pcrundata.tbgrpsku', N'pcrundata.tbgrpsku', N'GROUPSKU, SKUITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (35, N'pcrundata.tblftitm', N'pcrundata.tblftitm', N'ITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (36, N'pcrundata.pcpmtblpcn', N'pcrundata.pcpmtblpcn', N'PCCOMPANY, PCDIVISION, PCPC', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/location/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (37, N'ipcorpdta.tbfph', N'ipcorpdta.tbfph', N'PC, BRANCH, GROUPID, SKU', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (38, N'ipcorpdta.tbfpd', N'ipcorpdta.tbfpd', N'PC, BRANCH, GROUPID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (39, N'ipcorpdta.pcpmskustk', N'ipcorpdta.pcpmskustk', N'SKUSTKSKU', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (40, N'pcrundata.tborigin', N'pcrundata.tborigin', N'ORIGINID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (41, N'ipcorpdta.ippmvendor', N'ipcorpdta.ippmvendor', N'VENDORID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/vendor/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (42, N'pcrundata.tbfactry', N'pcrundata.tbfactry', N'FACTORYID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/factory/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (43, N'pcrundata.tbatrprd', N'pcrundata.tbatrprd', N'PRODID, GROUPID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (44, N'pcrundata.tbitmurl', N'pcrundata.tbitmurl', N'ITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (45, N'ipcorpdta.tbvendor', N'ipcorpdta.tbvendor', N'VENDOR', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/vendor/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (46, N'pcrundata.tbitemstat', N'pcrundata.tbitemstat', N'ITEM,EFFECTIVE_DATE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (47, N'pcrundata.tbusrnum', N'pcrundata.tbusrnum', N'HRID, TYPE, NUMBER, ACTIVEDT', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/employee/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (48, N'pcrundata.tbsca', N'pcrundata.tbsca', N'SCAID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/product/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (49, N'pcrundata.pcpmtblda', N'pcrundata.pcpmtblda', N'TBLDANUM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/delivery area/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (50, N'ipcorpdta.tbdwwrt', N'ipcorpdta.tbdwwrt', N'MARKET, SALENUM, LINE, TYPE, TRANTYPE, TRANSEQUENCE', N'WRITTENDATE', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (51, N'ipcorpdta.tbdwbld', N'ipcorpdta.tbdwbld', N'MARKET, SALENUM, LINE, TYPE, TRANTYPE, TRANSEQUENCE', N'BILLEDDATE', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (52, N'ipcorpdta.tbdwloc', N'ipcorpdta.tbdwloc', N'DIVISION, CENTER, BRANCHID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/location/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (53, N'ipcorpdta.tbdwemp', N'ipcorpdta.tbdwemp', N'EMPLOYEE_NUMBER', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/employee/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (54, N'ipcorpdta.tbdwwrtdel', N'ipcorpdta.tbdwwrt', N'MARKET, SALENUM, LINE, TYPE, TRANTYPE, TRANSEQUENCE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (55, N'ipcorpdta.tbdwblddel', N'ipcorpdta.tbdwbld', N'MARKET, SALENUM, LINE, TYPE, TRANTYPE, TRANSEQUENCE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/sales/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (56, N'pcrundata.tbdwpohdr', N'pcrundata.tbdwpohdr', N'DIV, PC, PONUM', N'UPDATED_TIMESTAMP', N'TIMESTAMP', N'2020-03-24 19:06:18.730', N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (57, N'pcrundata.tbdwpodtl', N'pcrundata.tbdwpodtl', N'DIV, PC, PONUM, POLINE', N'UPDATED_TIMESTAMP', N'TIMESTAMP', N'2020-03-24 19:13:28.153', N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (58, N'pcrundata.tbdwdckhdr', N'pcrundata.tbdwdckhdr', N'DCKDIV, DCKPC, DCKNBR', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (59, N'pcrundata.tbdwdckdtl', N'pcrundata.tbdwdckdtl', N'DCKDIV, DCKPC, DCKDTLNBR, DCKDTLPO, DCKDTLPOLN', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (60, N'pcrundata.tbdwpol', N'pcrundata.tbdwpol', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (61, N'pcrundata.tbdwpol000', N'pcrundata.tbdwpol000', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (62, N'pcrundata.tbdwpol001', N'pcrundata.tbdwpol001', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (63, N'pcrundata.tbdwpol002', N'pcrundata.tbdwpol002', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (64, N'pcrundata.tbdwpol003', N'pcrundata.tbdwpol003', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (65, N'pcrundata.tbdwpol004', N'pcrundata.tbdwpol004', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (66, N'pcrundata.tbdwpol005', N'pcrundata.tbdwpol005', N'PODIV, POPC, PONUM, POSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (67, N'pcrundata.tbdwsl', N'pcrundata.tbdwsl', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (68, N'pcrundata.tbdwsl001', N'pcrundata.tbdwsl001', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (69, N'pcrundata.tbdwsl002', N'pcrundata.tbdwsl002', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (70, N'pcrundata.tbdwsl003', N'pcrundata.tbdwsl003', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (71, N'pcrundata.tbdwsl004', N'pcrundata.tbdwsl004', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (72, N'pcrundata.tbdwsl005', N'pcrundata.tbdwsl005', N'DIV, PC, SHPKEY, SHPSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (73, N'pcrundata.tbshp', N'pcrundata.tbshp', N'DIV, PC, SHPKEY', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 1)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (74, N'nohalib.tbiaivt', N'nohalib.tbiaivt', N'RECTYPE, DATAKEY, PERIOD, DATATYPE, DIVISION, PC, BRANCH, ITEM', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/inventory/DB203002', N'PC03002', N'select CURRENT TIMESTAMP AS SNAPSHOT_DATE, RECTYPE, DATAKEY, PERIOD, DATATYPE, DIVISION, PC, BRANCH, ITEM, INVQTY, INVCOST, DMGQTY, DMGCOST, LINEUPSTAT from nohalib.tbiaivt i where rectype =''H''and datatype = ''SK''', 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (75, N'pcrundata.pcpmtblzip', N'pcrundata.pcpmtblzip', N'TBLZIPCODE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (76, N'pcrundata.tbsa', N'pcrundata.tbsa', N'SANUMBER', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/other/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (77, N'pcrundata.tbdwdsthdr', N'pcrundata.tbdwdsthdr', N'DSTHDRDIV, DSTHDRPC, DSTHDRNUM', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (78, N'pcrundata.tbdwdstdtl', N'pcrundata.tbdwdstdtl', N'DSTDTLDIV, DSTDTLPC, DSTDTLNUM, DSTDTLSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (79, N'pcrundata.tbdwasg', N'pcrundata.tbdwasg', N'DSTASGDNUM, DSTASGDSEQ, DSTASGSEQ, DSTASGDIV, DSTASGPC', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (80, N'pcrundata.tbdwsch', N'pcrundata.tbdwsch', N'DSTSCHDIV, DSTSCHPC, DSTSCHNUM', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (81, N'pcrundata.tbdwrte', N'pcrundata.tbdwrte', N'DSTRTEDIV, DSTRTEPC, DSTRTESCHN, DSTRTESEQN', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (82, N'pcrundata.tbdwstp', N'pcrundata.tbdwstp', N'DSTSTPDIV, DSTSTPPC, DSTSTPID', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (83, N'pcrundata.tbdwpochdr', N'pcrundata.tbdwpochdr', N'POCDIV, POCPC, PONUM, POCSEQ1', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (84, N'pcrundata.tbdwpocdt1', N'pcrundata.tbdwpocdt1', N'POCDIV, POCPC, PONUM, POCSEQ1', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (85, N'pcrundata.tbdwpocdt2', N'pcrundata.tbdwpocdt2', N'POCDIV, POCPC, PONUM, POCSEQ1, POCSEQ2', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (86, N'pcrundata.tbdwfif', N'pcrundata.tbdwfif', N'COMPANY, DIV, PC, PONUM, POLINE, POSEQ, GENER, UNICODE, RCVRTYPE, FRECTYPE', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (87, N'pcrundata.tbdwfifdel', N'pcrundata.tbdwfifdel', N'COMPANY, DIV, PC, PONUM, POLINE, POSEQ, GENER, UNICODE, RCVRTYPE, FRECTYPE', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/purchasing/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (88, N'pcrundata.tbdwicshdr', N'pcrundata.tbdwicshdr', N'ICSHDRDIV, ICSHDRPC, ICSNBR', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (89, N'pcrundata.tbdwicsdtl', N'pcrundata.tbdwicsdtl', N'ICSDTLDIV, ICSDTLPC, ICSNBR, ICSSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (90, N'pcrundata.tbdwicsahd', N'pcrundata.tbdwicsahd', N'ASGDIV, ASGPC, ASGNB', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (91, N'pcrundata.tbdwicsadt', N'pcrundata.tbdwicsadt', N'ASGDIV, ASGPC, ASGNBR, ASGICSNBR, ASGICSSEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (92, N'pcrundata.tbdwmnfhdr', N'pcrundata.tbdwmnfhdr', N'DSTMHDMACH, DSTMHDSCHN, DSTMHDSEQN, DSTMHDCKSQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [DB2SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (93, N'pcrundata.tbdwmnfdtl', N'pcrundata.tbdwmnfdtl', N'DSTMDTMACH, DSTMDTSCHN, DSTMDTSEQN, DSTMDTCKSQ, DSTMDTADNB, DSTMDTADSQ, DSTMDTASEQ', N'UPDATED_TIMESTAMP', N'TIMESTAMP', NULL, N'SELECT *', N'datalake/staging/distribution/DB203002', N'PC03002', NULL, 0, 0)
GO
SET IDENTITY_INSERT [ETL].[MetadataIngestionList] OFF
GO

INSERT [ETL].[MetadataIngestionControlDateRanges] ([DB2TableID], [DB2SchemaTable], [DB2ControlTable], [DB2ControlTableStartDateColumn], [DB2ControlTableEndDateColumn], [ControlProcessNumber], [ControlStartDate], [ControlEndDate]) VALUES (50, N'ipcorpdta.tbdwwrt', N'ipcorpdta.tbdwslsctl', N'extracted_from', N'extracted_thru', 300, CAST(N'2020-03-14T14:57:43.753' AS DateTime), CAST(N'2020-03-21T14:57:43.753' AS DateTime))
GO
INSERT [ETL].[MetadataIngestionControlDateRanges] ([DB2TableID], [DB2SchemaTable], [DB2ControlTable], [DB2ControlTableStartDateColumn], [DB2ControlTableEndDateColumn], [ControlProcessNumber], [ControlStartDate], [ControlEndDate]) VALUES (51, N'ipcorpdta.tbdwbld', N'ipcorpdta.tbdwslsctl', N'extracted_from', N'extracted_thru', 300, CAST(N'2020-03-14T14:57:43.750' AS DateTime), CAST(N'2020-03-21T14:57:43.750' AS DateTime))
GO

