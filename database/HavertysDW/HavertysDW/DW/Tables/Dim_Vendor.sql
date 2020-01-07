CREATE TABLE [DW].[Dim_Vendor] (
    [Vendor_sk]          INT          IDENTITY (1, 1) NOT NULL,
    [Vendor_ID_nk]       VARCHAR (3)  NOT NULL,
    [Vendor_Number]      INT          NOT NULL,
    [Vendor_Long_Name]   VARCHAR (40) NOT NULL,
    [Vendor_Short_Name]  VARCHAR (40) NOT NULL,
    [Vendor_Type_Code]   VARCHAR (2)  NOT NULL,
    [Vendor_Type_Name]   VARCHAR (25) NOT NULL,
    [Address_1]          VARCHAR (30) NOT NULL,
    [Address_2]          VARCHAR (30) NOT NULL,
    [City]               VARCHAR (20) NOT NULL,
    [State]              VARCHAR (2)  NOT NULL,
    [Zip]                VARCHAR (10) NOT NULL,
    [Country]            VARCHAR (10) NOT NULL,
    [Active]             BIT          NOT NULL,
    [IndirectDirect]     VARCHAR (9)  NOT NULL,
    [Status_ID]          VARCHAR (1)  NOT NULL,
    [Status]             VARCHAR (15) NOT NULL,
    [RTV_Credit_Allowed] VARCHAR (9)  NOT NULL,
    [HashKey]            CHAR (40)    NOT NULL,
    [SourceSystem_fk]    INT          NOT NULL,
    [ETLBatchID_Insert]  INT          NOT NULL,
    [ETLBatchID_Update]  INT          NOT NULL,
    CONSTRAINT [PK_Vendor_sk] PRIMARY KEY CLUSTERED ([Vendor_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);










GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Vendor_ID_nk]
    ON [DW].[Dim_Vendor]([Vendor_ID_nk] ASC) WITH (DATA_COMPRESSION = PAGE);



