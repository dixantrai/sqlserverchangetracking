USE [stageDB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE SCHEMA l1

CREATE TABLE [l1].[status](
	[src_obj_id] [INT] IDENTITY(1,1) NOT NULL,
	[src_obj_server] [VARCHAR](250) NOT NULL,
	[src_obj_db] [VARCHAR](250) NOT NULL,
	[src_obj_schema] [VARCHAR](250) NOT NULL,
	[src_obj] [VARCHAR](250) NOT NULL,	
	[stg_obj_server] [VARCHAR](250) NOT NULL,
	[stg_obj_db] [VARCHAR](250) NOT NULL,
	[stg_obj_schema] [VARCHAR](250) NOT NULL,
	[stg_obj] [VARCHAR](250) NOT NULL,	
	[sync_enabled] [BIT] NOT NULL,
	[sync_order] [INT] NOT NULL,
	[sync_status] [INT] NOT NULL,
	[sync_batch_dts] [DATETIME2](7) NULL,
	[sync_batch_ver] [VARCHAR](50) NULL,
	[sync_batch_ver_next] [VARCHAR](50) NULL,
	[sync_start_dts] [DATETIME2](7) NULL,
	[sync_end_dts] [DATETIME2](7) NULL,
	[sync_batch_row_count] [INT] NULL,
	[sync_log_msg] [VARCHAR](1000) NULL,
PRIMARY KEY CLUSTERED 
(
	[src_obj_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [l1].[status] ADD  DEFAULT ((0)) FOR [sync_enabled]
GO

ALTER TABLE [l1].[status] ADD  DEFAULT ((90)) FOR [sync_order]
GO

ALTER TABLE [l1].[status] ADD  DEFAULT ((0)) FOR [sync_status]
GO

INSERT INTO [l1].[status]
(	[src_obj_server],
	[src_obj_db],
	[src_obj_schema],
    [src_obj],
    [stg_obj_server],
	[stg_obj_db],
	[stg_obj_schema],
    [stg_obj],
	sync_enabled,
    sync_order,
    sync_status
)

SELECT 'TestServer' AS src_obj_server, 'srcDB' AS src_obj_db, 'dbo' src_obj_schema, 'testTable' src_obj, 'stageServer' AS stg_obj_server, 'stageDB' AS stg_obj_db, 'stage' stg_obj_schema, 'testTable' stg_obj, 1 sync_enabled, 1 sync_order, 1 sync_status

CREATE TABLE [l1].[src_objects_columns](
	[src_obj_id] [INT] NOT NULL,
	[src_db] [VARCHAR](250) NOT NULL,
	[src_schema] [VARCHAR](250) NOT NULL,
	[src_obj] [VARCHAR](250) NOT NULL,
	[src_column_name] [VARCHAR](250) NOT NULL,
	[sync_enabled] [BIT] NOT NULL,
	[PrimaryKey] [BIT] NOT NULL,
	[ORDINAL_POSITION] [INT] NULL,
	[COLUMN_DEFAULT] [NVARCHAR](4000) NULL,
	[IS_NULLABLE] [VARCHAR](3) NULL,
	[DATA_TYPE] [NVARCHAR](128) NULL,
	[CHARACTER_MAXIMUM_LENGTH] [INT] NULL,
	[NUMERIC_PRECISION] [TINYINT] NULL,
	[NUMERIC_SCALE] [TINYINT] NULL,
	[DATETIME_PRECISION] [SMALLINT] NULL,
	[COLLATION_NAME] [VARCHAR](250) NULL
) ON [PRIMARY]
GO

ALTER TABLE [stage].[src_objects_columns] ADD  DEFAULT ((0)) FOR [sync_enabled]
GO

ALTER TABLE [stage].[src_objects_columns] ADD  DEFAULT ((0)) FOR [PrimaryKey]
GO

CREATE TABLE [l2].[status](
	[tar_obj_id] [INT] IDENTITY(1,1) NOT NULL,
	[tar_obj_server] [VARCHAR](250) NOT NULL,
	[tar_obj_db] [VARCHAR](250) NOT NULL,
	[tar_obj_schema] [VARCHAR](250) NOT NULL,
	[tar_obj] [VARCHAR](250) NOT NULL,
	[stg_obj_server] [VARCHAR](250) NOT NULL,
	[stg_obj_db] [VARCHAR](250) NOT NULL,
	[stg_obj_schema] [VARCHAR](250) NOT NULL,
	[stg_obj] [VARCHAR](250) NOT NULL,
	[sync_enabled] [BIT] NOT NULL,
	[sync_order] [INT] NOT NULL,
	[sync_status] [INT] NOT NULL,
	[sync_batch] [VARCHAR](250) NOT NULL,
	[sync_batch_guid] [VARCHAR](50) NULL,
	[sync_start_dts] [DATETIME2](7) NULL,
	[sync_end_dts] [DATETIME2](7) NULL,
	[sync_batch_row_count] [INT] NULL,
	[sync_log_msg] [VARCHAR](1000) NULL,
PRIMARY KEY CLUSTERED 
(
	[tar_obj_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [l2].[status] ADD  DEFAULT ((0)) FOR [sync_enabled]
GO

ALTER TABLE [l2].[status] ADD  DEFAULT ((90)) FOR [sync_order]
GO

ALTER TABLE [l2].[status] ADD  DEFAULT ((0)) FOR [sync_status]
GO

INSERT INTO [l2].[status]
           ([tar_obj_server]
           ,[tar_obj_db]
           ,[tar_obj_schema]
           ,[tar_obj]
           ,[stg_obj_server]
           ,[stg_obj_db]
           ,[stg_obj_schema]
           ,[stg_obj]
           ,[sync_enabled]
           ,[sync_order]
		   ,sync_batch)

SELECT 
	'targetServer' AS [tar_obj_server],
	'targetDB' AS [tar_obj_db],
	'dbo' AS [tar_obj_db],
	'testTable' AS [tar_obj],
	stg_obj_server,
	stg_obj_db,
	stg_obj_schema,
	stg_obj,
	1 AS [sync_enabled],
	sync_order,
	'testSyncBatch' AS sync_batch
FROM l1.status
ORDER BY sync_order

CREATE TABLE [l2].[log](
	[ID] [BIGINT] IDENTITY(1,1) NOT NULL,
	[LogDTS] [DATETIME2](7) NULL,
	[batchGUID] [UNIQUEIDENTIFIER] NULL,
	[stg_obj_ref] [VARCHAR](500) NULL,
	[ErrorNumber] [INT] NULL,
	[ErrorMessage] [NVARCHAR](4000) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [l2].[log] ADD  DEFAULT (SYSDATETIME()) FOR [LogDTS]
GO

CREATE TABLE [l1].[status_log](
	[log_id] [BIGINT] IDENTITY(1,1) NOT NULL,
	[log_ldts] DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
	[src_obj_id] [INT] NOT NULL,
	[src_obj_server] [VARCHAR](250) NOT NULL,
	[src_obj_db] [VARCHAR](250) NOT NULL,
	[src_obj_schema] [VARCHAR](250) NOT NULL,
	[src_obj] [VARCHAR](250) NOT NULL,
	[stg_obj_server] [VARCHAR](250) NOT NULL,
	[stg_obj_db] [VARCHAR](250) NOT NULL,
	[stg_obj_schema] [VARCHAR](250) NOT NULL,
	[stg_obj] [VARCHAR](250) NOT NULL,
	[sync_enabled] [BIT] NOT NULL,
	[sync_order] [INT] NOT NULL,
	[sync_status] [INT] NOT NULL,
	[sync_batch_dts] [DATETIME2](7) NULL,
	[sync_batch_ver] [VARCHAR](50) NULL,
	[sync_batch_ver_next] [VARCHAR](50) NULL,
	[sync_start_dts] [DATETIME2](7) NULL,
	[sync_end_dts] [DATETIME2](7) NULL,
	[sync_batch_row_count] [INT] NULL,
	[sync_log_msg] [VARCHAR](1000) NULL
) ON [PRIMARY]
GO

CREATE TABLE [l2].[status_log](
	[log_id] [BIGINT] IDENTITY(1,1) NOT NULL,
	[log_ldts] DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
	[tar_obj_id] [INT] NOT NULL,
	[tar_obj_server] [VARCHAR](250) NOT NULL,
	[tar_obj_db] [VARCHAR](250) NOT NULL,
	[tar_obj_schema] [VARCHAR](250) NOT NULL,
	[tar_obj] [VARCHAR](250) NOT NULL,
	[stg_obj_server] [VARCHAR](250) NOT NULL,
	[stg_obj_db] [VARCHAR](250) NOT NULL,
	[stg_obj_schema] [VARCHAR](250) NOT NULL,
	[stg_obj] [VARCHAR](250) NOT NULL,
	[sync_enabled] [BIT] NOT NULL,
	[sync_order] [INT] NOT NULL,
	[sync_status] [INT] NOT NULL,
	[sync_batch] [VARCHAR](250) NOT NULL,
	[sync_batch_guid] [VARCHAR](50) NULL,
	[sync_start_dts] [DATETIME2](7) NULL,
	[sync_end_dts] [DATETIME2](7) NULL,
	[sync_batch_row_count] [INT] NULL,
	[sync_log_msg] [VARCHAR](1000) NULL
) ON [PRIMARY]
GO

ALTER TRIGGER [l1].[trg_status_afterUpdate]
	ON [l1].[status]
	AFTER UPDATE
AS 
BEGIN

	SET NOCOUNT ON;

	INSERT INTO [l1].[status_log]
		(
		[src_obj_id]
		,[src_obj_server]
		,[src_obj_db]
		,[src_obj_schema]
		,[src_obj]
		,[stg_obj_server]
		,[stg_obj_db]
		,[stg_obj_schema]
		,[stg_obj]
		,[sync_enabled]
		,[sync_order]
		,[sync_status]
		,[sync_batch_dts]
		,[sync_batch_ver]
		,[sync_batch_ver_next]
		,[sync_start_dts]
		,[sync_end_dts]
		,[sync_batch_row_count]
		,[sync_log_msg]
		)
	SELECT 
		[src_obj_id]
		,[src_obj_server]
		,[src_obj_db]
		,[src_obj_schema]
		,[src_obj]
		,[stg_obj_server]
		,[stg_obj_db]
		,[stg_obj_schema]
		,[stg_obj]
		,[sync_enabled]
		,[sync_order]
		,[sync_status]
		,[sync_batch_dts]
		,[sync_batch_ver]
		,[sync_batch_ver_next]
		,[sync_start_dts]
		,[sync_end_dts]
		,[sync_batch_row_count]
		,[sync_log_msg]
	FROM Inserted
	WHERE 1=1
	--AND [sync_status] = 1
	AND ([sync_batch_row_count] > 0 OR [sync_log_msg] NOT LIKE 'Success%')

END
GO

ALTER TRIGGER [l2].[trg_status_afterUpdate]
	ON [l2].[status]
	AFTER UPDATE
AS 
BEGIN

	SET NOCOUNT ON;

	INSERT INTO [l2].[status_log]
		(
		[tar_obj_id]
		,[tar_obj_server]
		,[tar_obj_db]
		,[tar_obj_schema]
		,[tar_obj]
		,[stg_obj_server]
		,[stg_obj_db]
		,[stg_obj_schema]
		,[stg_obj]
		,[sync_enabled]
		,[sync_order]
		,[sync_status]
		,[sync_batch]
		,[sync_batch_guid]
		,[sync_start_dts]
		,[sync_end_dts]
		,[sync_batch_row_count]
		,[sync_log_msg]
		)
	SELECT 
		[tar_obj_id]
		,[tar_obj_server]
		,[tar_obj_db]
		,[tar_obj_schema]
		,[tar_obj]
		,[stg_obj_server]
		,[stg_obj_db]
		,[stg_obj_schema]
		,[stg_obj]
		,[sync_enabled]
		,[sync_order]
		,[sync_status]
		,[sync_batch]
		,[sync_batch_guid]
		,[sync_start_dts]
		,[sync_end_dts]
		,[sync_batch_row_count]
		,[sync_log_msg]
	FROM Inserted
	WHERE 1=1
	AND ([sync_batch_row_count] > 0 OR [sync_log_msg] NOT LIKE 'Loading%')

END
GO

CREATE TRIGGER [l2].[trg_l2_error_log_afterInsert_Notification]
	ON [l2].[error_log]
	AFTER INSERT
AS 
BEGIN

	SET NOCOUNT ON;
	
	DECLARE @dbMailAccount AS VARCHAR(25), @recipientsOnFailure AS VARCHAR(MAX), @subject AS VARCHAR(1000), @body AS VARCHAR(4000)
	
	SET @dbMailAccount = 'DBA'
	SET @recipientsOnFailure = 'emailRecepient@email.com'
	SET @subject = QUOTENAME(@@SERVERNAME) + '.' + QUOTENAME(DB_NAME()) + ' : Data Sync Failure! Please investigate.'

	SELECT @body = 
		'LogDTS : ' + CAST(GETDATE() AS VARCHAR(50)) + CHAR(10) + 
		'Stage Object Reference: ' + [stg_obj_ref] + CHAR(10) + 
		'Error Number : ' + CAST([ErrorNumber] AS VARCHAR(50)) + CHAR(10) +
		'Error Message : ' + [ErrorMessage]
	FROM Inserted

	EXEC msdb..sp_send_dbmail 
		@profile_name = @dbMailAccount,
		@recipients = @recipientsOnFailure,
		@subject = @subject,
		@importance = 'High',
		@body = @body

END

GO

ALTER TABLE [l2].[error_log] ENABLE TRIGGER [trg_l2_error_log_afterInsert_Notification]
GO
