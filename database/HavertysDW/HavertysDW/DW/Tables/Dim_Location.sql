CREATE TABLE [DW].[Dim_Location] (
    [Location_sk]                      INT              IDENTITY (1, 1) NOT NULL,
    [Location_Code_nk]                 VARCHAR (15)     NOT NULL,
    [Division_ID]                      INT              NOT NULL,
    [Division]                         VARCHAR (50)     NOT NULL,
    [Region_ID]                        VARCHAR (4)      NOT NULL,
    [Region]                           VARCHAR (50)     NOT NULL,
    [Market_Code]                      INT              NOT NULL,
    [Market_ID]                        INT              NOT NULL,
    [Market]                           VARCHAR (50)     NOT NULL,
    [Location_ID]                      INT              NOT NULL,
    [Location_Name]                    VARCHAR (30)     NOT NULL,
    [Location_Sales_Manager]           VARCHAR (50)     NOT NULL,
    [Market_Manager]                   VARCHAR (50)     NOT NULL,
    [Label]                            VARCHAR (75)     NOT NULL,
    [Branch_Type]                      VARCHAR (2)      NOT NULL,
    [Location_Type]                    VARCHAR (2)      NULL,
    [Phone_Number]                     DECIMAL (10)     NOT NULL,
    [Address_1]                        VARCHAR (30)     NOT NULL,
    [Address_2]                        VARCHAR (30)     NOT NULL,
    [City]                             VARCHAR (20)     NOT NULL,
    [State]                            VARCHAR (2)      NOT NULL,
    [Country]                          VARCHAR (50)     NOT NULL,
    [Zip]                              VARCHAR (5)      NOT NULL,
    [Zip4]                             VARCHAR (4)      NULL,
    [Market_Zip]                       VARCHAR (5)      NULL,
    [Longitude]                        DECIMAL (16, 13) NOT NULL,
    [Latitude]                         DECIMAL (16, 13) NOT NULL,
    [Branch_Announced_Date]            DATE             NOT NULL,
    [Branch_Shut_Down_Date]            DATE             NOT NULL,
    [Active]                           VARCHAR (25)     NOT NULL,
    [Total_Square_Feet]                DECIMAL (10, 4)  NOT NULL,
    [Total_Sales_Square_Feet]          DECIMAL (10, 4)  NOT NULL,
    [Sales_Conditioned_CC_Square_Feet] DECIMAL (10, 4)  NOT NULL,
    [Sales_Square_Feet]                DECIMAL (10, 4)  NOT NULL,
    [Warehouse_Square_Feet]            DECIMAL (10, 4)  NOT NULL,
    [Office_Utility_Square_Feet]       DECIMAL (10, 4)  NOT NULL,
    [Admin_Flag]                       VARCHAR (1)      NOT NULL,
    [Size]                             VARCHAR (1)      NOT NULL,
    [Time_Zone]                        VARCHAR (5)      NOT NULL,
    [Accounting_Unit]                  VARCHAR (3)      NOT NULL,
    [Branch_Open_Date]                 DATE             NOT NULL,
    [Branch_Close_Date]                DATE             NOT NULL,
    [HashKey]                          CHAR (40)        NULL,
    [SourceSystem_fk]                  INT              NOT NULL,
    [ETLBatchID_Insert]                INT              NOT NULL,
    [ETLBatchID_Update]                INT              NOT NULL,
    CONSTRAINT [PK_Location_sk] PRIMARY KEY CLUSTERED ([Location_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);














GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Location_Code_nk]
    ON [DW].[Dim_Location]([Location_Code_nk] ASC) WITH (DATA_COMPRESSION = PAGE);



