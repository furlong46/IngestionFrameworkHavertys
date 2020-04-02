CREATE TABLE [DW].[Dim_DeliveryArea] (
    [Delivery_Area_sk]          INT           IDENTITY (1, 1) NOT NULL,
    [Delivery_Area_ID_nk]       INT           NOT NULL,
    [Delivery_Area_Type_Code]   VARCHAR (1)   NOT NULL,
    [Delivery_Area_Type]        VARCHAR (50)  NOT NULL,
    [Delivery_Area_Short_Name]  VARCHAR (50)  NOT NULL,
    [Delivery_Area_Name]        VARCHAR (100) NOT NULL,
    [Delivering_DC_ID]          INT           NOT NULL,
    [Delivering_DC]             VARCHAR (100) NOT NULL,
    [Drop_Off_Market_ID]        INT           NOT NULL,
    [Drop_Off_Market]           VARCHAR (100) NOT NULL,
    [Drop_Off_Location_ID]      INT           NOT NULL,
    [Drop_Off_Location]         VARCHAR (100) NOT NULL,
    [Serivce_Tech_Market_ID]    INT           NOT NULL,
    [Serivce_Tech_Market]       VARCHAR (100) NOT NULL,
    [Service_Tech_Location_ID]  INT           NOT NULL,
    [Service_Tech_Location]     VARCHAR (100) NOT NULL,
    [Customer_Pickup_Market_ID] INT           NOT NULL,
    [Customer_Pickup_Market]    VARCHAR (100) NOT NULL,
    [Active_Date]               DATE          NOT NULL,
    [Delivery_Area_Status]      VARCHAR (10)  NOT NULL,
    [HashKey]                   CHAR (40)     NOT NULL,
    [SourceSystem_fk]           INT           NOT NULL,
    [ETLBatchID_Insert]         INT           NOT NULL,
    [ETLBatchID_Update]         INT           NOT NULL,
    CONSTRAINT [PK_Delivery_Area_sk] PRIMARY KEY CLUSTERED ([Delivery_Area_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);





