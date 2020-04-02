CREATE TABLE [ETL].[JobConfigurationType] (
    [ConfigurationType] VARCHAR (50)  NOT NULL,
    [Description]       VARCHAR (500) NULL,
    CONSTRAINT [PK_JobConfigurationType] PRIMARY KEY CLUSTERED ([ConfigurationType] ASC)
);

