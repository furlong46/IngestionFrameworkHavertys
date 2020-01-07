






CREATE PROCEDURE [dbo].[usp_Load_Dim_SKU] 
AS

UPDATE Dest
SET
 Dest.[SKU_Description]				=	Staging.[SKU_Description]				
,Dest.[Vendor_Color_ID]				=	Staging.[Vendor_Color_ID]				
,Dest.[Vendor_Color]				=	Staging.[Vendor_Color]				
,Dest.[Vendor_Color_Hex_Code]		=	Staging.[Vendor_Color_Hex_Code]		
,Dest.[Vendor_Finish_ID]			=	Staging.[Vendor_Finish_ID]			
,Dest.[Vendor_Finish]				=	Staging.[Vendor_Finish]				
,Dest.[Configuration_ID]			=	Staging.[Configuration_ID]			
,Dest.[Configuration]				=	Staging.[Configuration]				
,Dest.[Display_Configuration]		=	Staging.[Display_Configuration]		
,Dest.[Size_ID]						=	Staging.[Size_ID]						
,Dest.[Size]						=	Staging.[Size]						
,Dest.[Original_Vendor_ID]			=	Staging.[Original_Vendor_ID]			
,Dest.[Original_Vendor_fk]			=	Staging.[Original_Vendor_fk]			
,Dest.[Collection_ID]				=	Staging.[Collection_ID]				
,Dest.[Collection]					=	Staging.[Collection]					
,Dest.[Group_ID]					=	Staging.[Group_ID]					
,Dest.[Group]						=	Staging.[Group]						
,Dest.[Product_ID]					=	Staging.[Product_ID]					
,Dest.[Product]						=	Staging.[Product]						
,Dest.[Reporting_Class_Code]		=	Staging.[Reporting_Class_Code]		
,Dest.[Reporting_Class]				=	Staging.[Reporting_Class]				
,Dest.[Super_Category_ID]			=	Staging.[Super_Category_ID]			
,Dest.[Super_Category]				=	Staging.[Super_Category]				
,Dest.[Category_ID]					=	Staging.[Category_ID]					
,Dest.[Category]					=	Staging.[Category]					
,Dest.[Sub_Category_ID]				=	Staging.[Sub_Category_ID]				
,Dest.[Sub_Category]				=	Staging.[Sub_Category]				
,Dest.[Current_EDLP_Price]			=	Staging.[Current_EDLP_Price]			
,Dest.[Good_Better_Best_Category]	=	Staging.[Good_Better_Best_Category]	
,Dest.[GBB_Price_Point]				=	Staging.[GBB_Price_Point]				
,Dest.[Motion]						=	Staging.[Motion]						
,Dest.[Dining_Style]				=	Staging.[Dining_Style]				
,Dest.[Bedroom_Style]				=	Staging.[Bedroom_Style]				
,Dest.[Standalone_bed]				=	Staging.[Standalone_bed]				
,Dest.[SKU_Type]					=	Staging.[SKU_Type]					
,Dest.[Stocked]						=	Staging.[Stocked]						
,Dest.[Type_of_SKU_Code]			=	Staging.[Type_of_SKU_Code]					
,Dest.[Type_of_SKU]					=	Staging.[Type_of_SKU]					
,Dest.[Vendor_ID]					=	Staging.[Vendor_ID]						
,Dest.[Vendor_fk]					=	Staging.[Vendor_fk]					
,Dest.[Factory_ID]					=	Staging.[Factory_ID]						
,Dest.[Factory_fk]					=	Staging.[Factory_fk]					
,Dest.[Special_Order_Available]		=	Staging.[Special_Order_Available]		
,Dest.[Vendor_Lead_Time]			=	Staging.[Vendor_Lead_Time]			
,Dest.[Product_Lead_Time]			=	Staging.[Product_Lead_Time]			
,Dest.[Group_Active_Start]			=	Staging.[Group_Active_Start]			
,Dest.[Group_Active_End]			=	Staging.[Group_Active_End]			
,Dest.[SKU_Flow_StartDate]			=	Staging.[SKU_Flow_StartDate]			
,Dest.[SKU_Flow_EndDate]			=	Staging.[SKU_Flow_EndDate]			
,Dest.[Stocked_at_EDC]				=	Staging.[Stocked_at_EDC]				
,Dest.[Stocked_at_WDC]				=	Staging.[Stocked_at_WDC]				
,Dest.[Stocked_at_FDC]				=	Staging.[Stocked_at_FDC]				
,Dest.[Group_Web_Status]			=	Staging.[Group_Web_Status]			
,Dest.[SKU_Web_Status]				=	Staging.[SKU_Web_Status]				
,Dest.[Merchandising_Approval]		=	Staging.[Merchandising_Approval]		
,Dest.[Marketing_Approval]			=	Staging.[Marketing_Approval]			
,Dest.[Lineup_Status_Code]			=	Staging.[Lineup_Status_Code]	
,Dest.[Lineup_Status]				=	Staging.[Lineup_Status]				
,Dest.[Available_to_Purchase]		=	Staging.[Available_to_Purchase]		
,Dest.[Ship_Length]					=	Staging.[Ship_Length]					
,Dest.[Ship_Width]					=	Staging.[Ship_Width]					
,Dest.[Ship_Height]					=	Staging.[Ship_Height]					
,Dest.[Length]						=	Staging.[Length]						
,Dest.[Width]						=	Staging.[Width]						
,Dest.[Height]						=	Staging.[Height]						
,Dest.[Weight]						=	Staging.[Weight]						
,Dest.[Prep_Cubes]					=	Staging.[Prep_Cubes]					
,Dest.[Cubes]						=	Staging.[Cubes]						
,Dest.[Prep_Flag]					=	Staging.[Prep_Flag]					
,Dest.[Assembly_Required]			=	Staging.[Assembly_Required]			
,Dest.[Assembly_Time]				=	Staging.[Assembly_Time]				
,Dest.[Leave_in_Box]				=	Staging.[Leave_in_Box]				
,Dest.[Manufacturer_ID]				=	Staging.[Manufacturer_ID]				
,Dest.[UPC]							=	Staging.[UPC]							
,Dest.[HTS_Number]					=	Staging.[HTS_Number]					
,Dest.[Original_SKU]				=	Staging.[Original_SKU]				
,Dest.[Representative_Item]			=	Staging.[Representative_Item]			
,Dest.[Group_Rating_Count]			=	Staging.[Group_Rating_Count]			
,Dest.[Product_Rating_Count]		=	Staging.[Product_Rating_Count]		
,Dest.[Group_Star_Rating]			=	Staging.[Group_Star_Rating]			
,Dest.[Product_Star_Rating]			=	Staging.[Product_Star_Rating]			
,Dest.[Made_in_ID]					=	Staging.[Made_in_ID]					
,Dest.[Made_in_City]				=	Staging.[Made_in_City]				
,Dest.[Made_in_State]				=	Staging.[Made_in_State]				
,Dest.[Made_in_Country]				=	Staging.[Made_in_Country]				
,Dest.[Origin_ID]					=	Staging.[Origin_ID]					
,Dest.[Origin_City]					=	Staging.[Origin_City]					
,Dest.[Origin_State]				=	Staging.[Origin_State]				
,Dest.[Origin_Country]				=	Staging.[Origin_Country]				
,Dest.[Copy]						=	Staging.[Copy]						
,Dest.[Web_Description]				=	Staging.[Web_Description]				
,Dest.[Price_Tag_Description]		=	Staging.[Price_Tag_Description]		
,Dest.[Primary_Common_Color]		=	Staging.[Primary_Common_Color]		
,Dest.[Primary_Common_Finish]		=	Staging.[Primary_Common_Finish]		
,Dest.[Floor_Sample_Bedding]		=	Staging.[Floor_Sample_Bedding]		
,Dest.[Employee_Bedding]			=	Staging.[Employee_Bedding]			
,Dest.[Accessory_Story]				=	Staging.[Accessory_Story]	
,Dest.[IndirectDirect]				=	Staging.[IndirectDirect]	
,Dest.[3D_Model_Required]			=	Staging.[3D_Model_Required]			
,Dest.[3D_Model]					=	Staging.[3D_Model]					
,Dest.[Scene_7_Image_URL]			=	Staging.[Scene_7_Image_URL]			
,Dest.[Havertys.com_URL]			=	Staging.[Havertys.com_URL]			
,Dest.[Advertised_Price]			=	Staging.[Advertised_Price]			
,Dest.[HashKey]						=	Staging.[HashKey]
,Dest.[ETLBatchID_Update]			=	Staging.[ETLBatchID_Update]
FROM 
[DW].[Dim_SKU] Dest INNER JOIN [Updates].[Dim_SKU_Update] Staging ON Dest.SKU_sk = Staging.SKU_sk_Dest


UPDATE S
SET S.Vendor_fk = V.Vendor_sk
FROM
[DW].[Dim_SKU] S
INNER JOIN DW.Dim_Vendor V ON S.Vendor_ID = V.Vendor_ID_nk
WHERE S.Vendor_fk = 0

UPDATE S
SET S.Factory_fk = F.Factory_sk
FROM
[DW].[Dim_SKU] S
INNER JOIN DW.Dim_Factory F ON S.Factory_ID = F.Factory_Code_nk
WHERE S.Factory_fk = 0

UPDATE S
SET S.Original_Vendor_fk = V.Vendor_sk
FROM
[DW].[Dim_SKU] S
INNER JOIN DW.Dim_Vendor V ON S.Original_Vendor_ID = V.Vendor_ID_nk
WHERE S.Original_Vendor_fk = 0