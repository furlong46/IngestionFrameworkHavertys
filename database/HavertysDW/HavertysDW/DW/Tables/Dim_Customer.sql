CREATE TABLE [DW].[Dim_Customer] (
    [Customer_sk]                   INT            IDENTITY (1, 1) NOT NULL,
    [Customer_Sequence_nk]          INT            NOT NULL,
    [Customer_ID_nk]                BIGINT         NOT NULL,
    [Sequence_Status]               VARCHAR (1)    NOT NULL,
    [Bill_First_Name]               VARCHAR (30)   NOT NULL,
    [Bill_Last_Name]                VARCHAR (30)   NOT NULL,
    [Bill_Name_Status]              VARCHAR (1)    NOT NULL,
    [Bill_Address]                  VARCHAR (64)   NOT NULL,
    [Bill_City]                     VARCHAR (20)   NOT NULL,
    [Bill_State]                    VARCHAR (2)    NOT NULL,
    [Bill_ZIP]                      VARCHAR (20)   NOT NULL,
    [Bill_ZIP4]                     VARCHAR (20)   NOT NULL,
    [Bill_Latitude]                 DECIMAL (9, 6) NOT NULL,
    [Bill_Longitude]                DECIMAL (9, 6) NOT NULL,
    [Bill_Address_Status]           VARCHAR (1)    NOT NULL,
    [Delivery_First_Name]           VARCHAR (30)   NOT NULL,
    [Delivery_Last_Name]            VARCHAR (30)   NOT NULL,
    [Delivery_Name_Status]          VARCHAR (1)    NOT NULL,
    [Delivery_Address]              VARCHAR (64)   NOT NULL,
    [Delivery_City]                 VARCHAR (20)   NOT NULL,
    [Delivery_State]                VARCHAR (2)    NOT NULL,
    [Delivery_ZIP]                  VARCHAR (20)   NOT NULL,
    [Delivery_ZIP4]                 VARCHAR (20)   NOT NULL,
    [Delivery_Latitude]             DECIMAL (9, 6) NOT NULL,
    [Delivery_Longitude]            DECIMAL (9, 6) NOT NULL,
    [Delivery_Address_Status]       VARCHAR (1)    NOT NULL,
    [Customer_Profile_Created_Date] DATE           NOT NULL,
    [Customer_First_Name]           VARCHAR (30)   NOT NULL,
    [Customer_Last_Name]            VARCHAR (30)   NOT NULL,
    [Customer_Name_Status]          VARCHAR (1)    NOT NULL,
    [Customer_Address]              VARCHAR (64)   NOT NULL,
    [Customer_City]                 VARCHAR (20)   NOT NULL,
    [Customer_State]                VARCHAR (2)    NOT NULL,
    [Customer_ZIP]                  VARCHAR (20)   NOT NULL,
    [Customer_ZIP4]                 VARCHAR (20)   NOT NULL,
    [Customer_Latitude]             DECIMAL (9, 6) NOT NULL,
    [Customer_Longitude]            DECIMAL (9, 6) NOT NULL,
    [Customer_Phone_Type]           VARCHAR (1)    NOT NULL,
    [Customer_Phone]                VARCHAR (10)   NOT NULL,
    [Customer_Phone_Extension]      VARCHAR (10)   NOT NULL,
    [Customer_Phone_Status]         VARCHAR (1)    NOT NULL,
    [Customer_Email]                VARCHAR (50)   NOT NULL,
    [Customer_Email_Status]         VARCHAR (1)    NOT NULL,
    [Customer_Created_Date]         DATE           NOT NULL,
    [Customer_Status]               VARCHAR (1)    NOT NULL,
    [No_Email]                      VARCHAR (1)    NOT NULL,
    [Room_Plan]                     VARCHAR (1)    NOT NULL,
    [Send_Advertising]              VARCHAR (1)    NOT NULL,
    [Send_Catalog]                  VARCHAR (1)    NOT NULL,
    [Send_News]                     VARCHAR (1)    NOT NULL,
    [Send_P_Mail]                   VARCHAR (1)    NOT NULL,
    [HashKey]                       CHAR (40)      NOT NULL,
    [SourceSystem_fk]               INT            NOT NULL,
    [ETLBatchID_Insert]             INT            NOT NULL,
    [ETLBatchID_Update]             INT            NOT NULL,
    CONSTRAINT [PK_Customer_sk] PRIMARY KEY NONCLUSTERED ([Customer_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);












GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Customer_Sequence_nk_Customer_ID_nk]
    ON [DW].[Dim_Customer]([Customer_Sequence_nk] ASC, [Customer_ID_nk] ASC)
    INCLUDE([Customer_sk], [ETLBatchID_Insert]) WITH (DATA_COMPRESSION = PAGE);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Dim_Customer]
    ON [DW].[Dim_Customer];


GO
CREATE NONCLUSTERED INDEX [IX_Customer_Created_Date]
    ON [DW].[Dim_Customer]([Customer_Created_Date] ASC) WITH (DATA_COMPRESSION = PAGE);



