CREATE TABLE [DW].[Dim_Factory] (
    [Factory_sk]        INT            IDENTITY (1, 1) NOT NULL,
    [Factory_Code_nk]   INT            NOT NULL,
    [Factory_Name]      VARCHAR (100)  NOT NULL,
    [Address_1]         VARCHAR (50)   NOT NULL,
    [Address_2]         VARCHAR (50)   NOT NULL,
    [City]              VARCHAR (50)   NOT NULL,
    [State]             VARCHAR (50)   NOT NULL,
    [Zip]               VARCHAR (10)   NULL,
    [Country_Code]      CHAR (2)       NULL,
    [Country]           VARCHAR (30)   NOT NULL,
    [Active]            VARCHAR (1)    NOT NULL,
    [Latitude]          NUMERIC (9, 6) NULL,
    [Longitude]         NUMERIC (9, 6) NULL,
    [Created_Date]      DATE           NULL,
    [HashKey]           CHAR (40)      NOT NULL,
    [SourceSystem_fk]   INT            NOT NULL,
    [ETLBatchID_Insert] INT            NOT NULL,
    [ETLBatchID_Update] INT            NOT NULL,
    CONSTRAINT [PK_Factory_sk] PRIMARY KEY CLUSTERED ([Factory_sk] ASC) WITH (DATA_COMPRESSION = PAGE)
);












GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_Factory_Code_nk]
    ON [DW].[Dim_Factory]([Factory_Code_nk] ASC) WITH (DATA_COMPRESSION = PAGE);



