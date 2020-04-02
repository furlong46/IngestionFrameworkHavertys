CREATE TABLE [ETL].[BlobFileMetadataIngestionList] (
    [BlobFileID]            INT            IDENTITY (1, 1) NOT NULL,
    [BlobFolder]            VARCHAR (1000) NOT NULL,
    [FileName]              VARCHAR (50)   NOT NULL,
    [BlobFileType]          VARCHAR (50)   NOT NULL,
    [DataLakeStagingFolder] VARCHAR (1000) NOT NULL,
    [Description]           VARCHAR (1000) NULL,
    CONSTRAINT [PK_BlobFileMetadataIngestionList] PRIMARY KEY CLUSTERED ([BlobFileID] ASC)
);

