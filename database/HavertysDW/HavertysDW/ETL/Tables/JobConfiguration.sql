CREATE TABLE [ETL].[JobConfiguration] (
    [JobConfigurationID] INT          IDENTITY (1, 1) NOT NULL,
    [JobConfiguration]   VARCHAR (50) NOT NULL,
    [TableControlType]   VARCHAR (10) CONSTRAINT [DF_JobConfiguration_TableControlType] DEFAULT ('Azure') NOT NULL,
    [TableID]            INT          NOT NULL,
    [SchemaTable]        VARCHAR (50) NOT NULL,
    [ExecuteFlag]        BIT          CONSTRAINT [DF_JobConfiguration_ExecuteFlag] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_JobConfiguration] PRIMARY KEY CLUSTERED ([JobConfigurationID] ASC),
    CONSTRAINT [CH_JobConfiguration_TableControlType] CHECK ([TableControlType]='Azure' OR [TableControlType]='DW'),
    FOREIGN KEY ([JobConfiguration]) REFERENCES [ETL].[JobConfigurationType] ([ConfigurationType])
);
















GO
CREATE UNIQUE NONCLUSTERED INDEX [IUX_JobConfiguration]
    ON [ETL].[JobConfiguration]([JobConfiguration] ASC, [TableControlType] ASC, [TableID] ASC);

