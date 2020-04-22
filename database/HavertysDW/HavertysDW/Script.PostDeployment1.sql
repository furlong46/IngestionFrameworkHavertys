SET IDENTITY_INSERT [ETL].[MetadataIngestionList] ON 
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (8, N'SalesLT.Address', N'SalesLT.Address', N'AddressID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (9, N'SalesLT.Customer', N'SalesLT.Customer', N'CustomerID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (10, N'SalesLT.CustomerAddress', N'SalesLT.CustomerAddress', N'CustomerID, AddressID', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (11, N'SalesLT.Product', N'SalesLT.Product', N'ProductID', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (12, N'SalesLT.ProductCategory', N'SalesLT.ProductCategory', N'ProductCategoryID', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (13, N'SalesLT.ProductDescription', N'SalesLT.ProductDescription', N'ProductDescriptionID', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (14, N'SalesLT.ProductModel', N'SalesLT.ProductModel', N'ProductModelID', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (15, N'SalesLT.ProductModelProductDescription', N'SalesLT.ProductModelProductDescription', N'ProductModelID, ProductDescriptionID, Culture', N'ModifiedDate', N'TIMESTAMP', N'2000-01-01', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (50, N'SalesLT.SalesOrderDetail', N'SalesLT.SalesOrderDetail', N'SalesOrderID, SalesOrderDetailID', N'ModifiedDate', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/erp', N'PC03002', NULL, 0, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [Disabled]) VALUES (51, N'SalesLT.SalesOrderHeader', N'SalesLT.SalesOrderHeader', N'SalesOrderID', N'OrderDate', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/erp', N'PC03002', NULL, 0, 0)
GO
SET IDENTITY_INSERT [ETL].[MetadataIngestionList] OFF
GO
INSERT [ETL].[MetadataIngestionControlDateRanges] ([TableID], [SchemaTable], [ControlTable], [ControlTableStartDateColumn], [ControlTableEndDateColumn], [ControlProcessNumber], [ControlStartDate], [ControlEndDate]) VALUES (50, N'SalesLT.SalesOrderDetail', N'SalesLT.SalesOrderDetail', N'ModifiedDate', N'ModifiedDate', 300, CAST(N'2001-03-14T14:57:43.753' AS DateTime), CAST(N'2021-03-21T14:57:43.753' AS DateTime))
GO
INSERT [ETL].[MetadataIngestionControlDateRanges] ([TableID], [SchemaTable], [ControlTable], [ControlTableStartDateColumn], [ControlTableEndDateColumn], [ControlProcessNumber], [ControlStartDate], [ControlEndDate]) VALUES (51, N'SalesLT.SalesOrderHeader', N'SalesLT.SalesOrderHeader', N'OrderDate', N'OrderDate', 300, CAST(N'2001-03-14T14:57:43.753' AS DateTime), CAST(N'2021-03-21T14:57:43.750' AS DateTime))
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'InventoryNightly', N'Run the nightly inventory refresh.')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'InventoryOverride', N'Run the historical inventory refresh.')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'NightlyLoad', N'Runs every night and performs entire ETL process')
GO
INSERT [ETL].[JobConfigurationType] ([ConfigurationType], [Description]) VALUES (N'OverrideLoad', N'Runs on-demand and performs entire ETL process')
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] ON 
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (251, N'NightlyLoad', N'DB2', 8, N'SalesLT.Address', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (252, N'NightlyLoad', N'DB2', 9, N'SalesLT.Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (253, N'NightlyLoad', N'DB2', 10, N'SalesLT.CustomerAddress', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (254, N'NightlyLoad', N'DB2', 11, N'SalesLT.Product', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (255, N'NightlyLoad', N'DB2', 12, N'SalesLT.ProductCategory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (256, N'NightlyLoad', N'DB2', 13, N'SalesLT.ProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (257, N'NightlyLoad', N'DB2', 14, N'SalesLT.ProductModel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (258, N'NightlyLoad', N'DB2', 15, N'SalesLT.ProductModelProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (344, N'OverrideLoad', N'DB2', 8, N'SalesLT.Address', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (345, N'OverrideLoad', N'DB2', 9, N'SalesLT.Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (346, N'OverrideLoad', N'DB2', 10, N'SalesLT.CustomerAddress', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (347, N'OverrideLoad', N'DB2', 11, N'SalesLT.Product', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (348, N'OverrideLoad', N'DB2', 12, N'SalesLT.ProductCategory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (349, N'OverrideLoad', N'DB2', 13, N'SalesLT.ProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (350, N'OverrideLoad', N'DB2', 14, N'SalesLT.ProductModel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (351, N'OverrideLoad', N'DB2', 15, N'SalesLT.ProductModelProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (386, N'OverrideLoad', N'DB2', 50, N'SalesLT.SalesOrderDetail', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (387, N'OverrideLoad', N'DB2', 51, N'SalesLT.SalesOrderHeader', 1)
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] OFF
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'NightlyLoad', 10)
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'OverrideLoad', 10)
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'CDCOffsetDays', N'3')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AnalysisServicesServer', N'asazure://eastus.asazure.windows.net/dataanalyticsqaaas21')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLServerName', N'dataanalyticsqa-sql21')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLDatabaseName', N'HavertysDW_QA')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SubscriptionID', N'e2f53281-fd58-437e-82e1-088d1b79fcbc')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'ResourceGroupName', N'DataAnalytics_QA_RG')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookFull', N'https://s16events.azure-automation.net/webhooks?token=F2xAYLKuX%2fs1VY%2byw7QFUIL0cQal1QKrBmrISNEKBBU%3d')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookIncremental', N'https://s16events.azure-automation.net/webhooks?token=t7GUWw%2bHL81EfJlHJhqaFSfrB6dkjELv0yWdTjTx7TI%3d')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SQLComputeSizeHigh', N'GP_Gen5_16')
GO
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
