CREATE TABLE [test].[factory] (
    [Factory_Code_nk]   INT            NULL,
    [Factory_Name]      NVARCHAR (MAX) NOT NULL,
    [Address_1]         NVARCHAR (MAX) NOT NULL,
    [Address_2]         NVARCHAR (MAX) NOT NULL,
    [City]              NVARCHAR (MAX) NOT NULL,
    [State]             NVARCHAR (MAX) NOT NULL,
    [Zip]               NVARCHAR (MAX) NULL,
    [Country]           NVARCHAR (MAX) NOT NULL,
    [Active]            NVARCHAR (MAX) NOT NULL,
    [HashKey]           NVARCHAR (MAX) NULL,
    [SourceSystem_fk]   INT            NOT NULL,
    [ETLBatchID_Insert] INT            NOT NULL,
    [ETLBatchID_Update] INT            NOT NULL,
    [Latitude]          DECIMAL (9, 6) NULL,
    [Longitude]         DECIMAL (9, 6) NULL,
    [Country_Code]      NVARCHAR (MAX) NOT NULL,
    [Created_Date]      DATE           NULL
);

