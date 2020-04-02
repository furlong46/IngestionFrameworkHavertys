CREATE TABLE [Updates].[Dim_Employee_Update] (
    [employee_id_nk]         NVARCHAR (MAX)   NULL,
    [employeetype_code]      NVARCHAR (MAX)   NULL,
    [employeetype_name]      NVARCHAR (MAX)   NOT NULL,
    [sales_id]               DECIMAL (38, 18) NOT NULL,
    [wms_id]                 DECIMAL (38, 18) NOT NULL,
    [driver_id]              DECIMAL (38, 18) NOT NULL,
    [designer_id]            DECIMAL (38, 18) NOT NULL,
    [first_name]             NVARCHAR (MAX)   NULL,
    [last_name]              NVARCHAR (MAX)   NULL,
    [full_name]              NVARCHAR (MAX)   NULL,
    [division_id]            DECIMAL (38, 18) NULL,
    [market_id]              DECIMAL (38, 18) NULL,
    [branch_id]              DECIMAL (38, 18) NULL,
    [common_name]            NVARCHAR (MAX)   NULL,
    [HashKey]                NVARCHAR (MAX)   NULL,
    [SourceSystem_fk]        INT              NOT NULL,
    [ETLBatchID_Insert]      INT              NOT NULL,
    [ETLBatchID_Update]      INT              NOT NULL,
    [Employee_sk_Dest]       INT              NULL,
    [Employee_ID_nk_Dest]    NVARCHAR (MAX)   NULL,
    [HashKey_Dest]           NVARCHAR (MAX)   NULL,
    [ETLBatchID_Insert_Dest] INT              NULL
)
WITH (DATA_COMPRESSION = PAGE);





