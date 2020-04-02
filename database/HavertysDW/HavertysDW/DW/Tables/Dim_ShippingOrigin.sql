CREATE TABLE [DW].[Dim_ShippingOrigin] (
    [Shipping_Origin_sk]               INT            IDENTITY (1, 1) NOT NULL,
    [Shipping_Origin_ID_nk]            INT            NOT NULL,
    [Shipping_Origin_Code]             VARCHAR (5)    NOT NULL,
    [Shipping_Origin_UN_Location_Code] VARCHAR (5)    NOT NULL,
    [Shipping_Origin_Name]             VARCHAR (75)   NOT NULL,
    [Shipping_Origin_Type_Code]        VARCHAR (1)    NOT NULL,
    [Shipping_Origin_Type]             VARCHAR (50)   NOT NULL,
    [City]                             VARCHAR (50)   NOT NULL,
    [State_Code]                       VARCHAR (2)    NOT NULL,
    [State]                            VARCHAR (50)   NOT NULL,
    [Country_Code]                     VARCHAR (2)    NOT NULL,
    [Country]                          VARCHAR (50)   NOT NULL,
    [Latitude]                         NUMERIC (9, 6) NOT NULL,
    [Longitude]                        NUMERIC (9, 6) NOT NULL,
    [HashKey]                          CHAR (40)      NOT NULL,
    [SourceSystem_fk]                  INT            NOT NULL,
    [ETLBatchID_Insert]                INT            NOT NULL,
    [ETLBatchID_Update]                INT            NOT NULL,
    CONSTRAINT [PK_Shipping_Origin_sk] PRIMARY KEY CLUSTERED ([Shipping_Origin_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);



