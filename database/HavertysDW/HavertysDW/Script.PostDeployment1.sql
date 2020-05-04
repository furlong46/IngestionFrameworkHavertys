SET IDENTITY_INSERT [ETL].[MetadataIngestionList] ON 
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (8, N'SalesLT.Address', N'SalesLT.Address', N'AddressID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 0, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (9, N'SalesLT.Customer', N'SalesLT.Customer', N'CustomerID', NULL, NULL, NULL, N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 0, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (10, N'SalesLT.CustomerAddress', N'SalesLT.CustomerAddress', N'CustomerID, AddressID', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:49:48.840', N'SELECT *', N'datalake/staging/master data/customer/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (11, N'SalesLT.Product', N'SalesLT.Product', N'ProductID', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:49:48.840', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (12, N'SalesLT.ProductCategory', N'SalesLT.ProductCategory', N'ProductCategoryID', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:49:48.840', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (13, N'SalesLT.ProductDescription', N'SalesLT.ProductDescription', N'ProductDescriptionID', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:50:41.150', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (14, N'SalesLT.ProductModel', N'SalesLT.ProductModel', N'ProductModelID', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:50:44.520', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (15, N'SalesLT.ProductModelProductDescription', N'SalesLT.ProductModelProductDescription', N'ProductModelID, ProductDescriptionID, Culture', N'ModifiedDate', N'TIMESTAMP', N'2020-04-23 14:57:31.163', N'SELECT *', N'datalake/staging/master data/product/erp', N'PC03002', NULL, 1, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (50, N'SalesLT.SalesOrderDetail', N'SalesLT.SalesOrderDetail', N'SalesOrderID, SalesOrderDetailID', N'ModifiedDate', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/erp', N'PC03002', NULL, 0, 1, 0)
GO
INSERT [ETL].[MetadataIngestionList] ([TableID], [SchemaTable], [DataLakeSchemaTable], [PKColumnList], [FilterColumn], [FilterColumnDataType], [FilterColumnValue], [SelectQuery], [DataLakeStagingFolder], [ServerName], [SqlOverrideQuery], [LoadCurated], [CreateRawTable], [Disabled]) VALUES (51, N'SalesLT.SalesOrderHeader', N'SalesLT.SalesOrderHeader', N'SalesOrderID', N'OrderDate', N'DATE', NULL, N'SELECT *', N'datalake/staging/sales/erp', N'PC03002', NULL, 0, 1, 0)
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
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (251, N'NightlyLoad', N'Azure', 8, N'SalesLT.Address', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (252, N'NightlyLoad', N'Azure', 9, N'SalesLT.Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (253, N'NightlyLoad', N'Azure', 10, N'SalesLT.CustomerAddress', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (254, N'NightlyLoad', N'Azure', 11, N'SalesLT.Product', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (255, N'NightlyLoad', N'Azure', 12, N'SalesLT.ProductCategory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (256, N'NightlyLoad', N'Azure', 13, N'SalesLT.ProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (257, N'NightlyLoad', N'Azure', 14, N'SalesLT.ProductModel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (258, N'NightlyLoad', N'Azure', 15, N'SalesLT.ProductModelProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (344, N'OverrideLoad', N'Azure', 8, N'SalesLT.Address', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (345, N'OverrideLoad', N'Azure', 9, N'SalesLT.Customer', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (346, N'OverrideLoad', N'Azure', 10, N'SalesLT.CustomerAddress', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (347, N'OverrideLoad', N'Azure', 11, N'SalesLT.Product', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (348, N'OverrideLoad', N'Azure', 12, N'SalesLT.ProductCategory', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (349, N'OverrideLoad', N'Azure', 13, N'SalesLT.ProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (350, N'OverrideLoad', N'Azure', 14, N'SalesLT.ProductModel', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (351, N'OverrideLoad', N'Azure', 15, N'SalesLT.ProductModelProductDescription', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (386, N'OverrideLoad', N'Azure', 50, N'SalesLT.SalesOrderDetail', 1)
GO
INSERT [ETL].[JobConfiguration] ([JobConfigurationID], [JobConfiguration], [TableControlType], [TableID], [SchemaTable], [ExecuteFlag]) VALUES (387, N'OverrideLoad', N'Azure', 51, N'SalesLT.SalesOrderHeader', 1)
GO
SET IDENTITY_INSERT [ETL].[JobConfiguration] OFF
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'NightlyLoad', 10)
GO
INSERT [ETL].[JobParallelism] ([JobConfiguration], [JobParallelism]) VALUES (N'OverrideLoad', 10)
GO

INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'CDCOffsetDays', N'3')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AnalysisServicesServer', N'asazure://eastus.asazure.windows.net/<servername>')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLServerName', N'sqlserver-mdp-demo')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'AzureSQLDatabaseName', N'sql-mdp-demo')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SubscriptionID', N'75bd89de-4fa2-41ad-9b88-b2b7360945fe')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'ResourceGroupName', N'MDP-Demo')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookFull', N'https://')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'WebHookIncremental', N'https://')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SQLComputeSizeHigh', N'GP_S_Gen5_1')
GO
INSERT [ETL].[Configurations] ([ConfigKey], [ConfigValue]) VALUES (N'SQLComputeSizeLow', N'GP_S_Gen5_1')
GO
