USE stageDB
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/******************************************************************************************************
Author:			Dixant Rai
Create date:	2019-10-15
Description:	
--------------------------------------------------------------------------------------------------------
Revision History

Version		Date				Author						Comment
--------------------------------------------------------------------------------------------------------
1.0			2019-10-15			Dixant Rai					Created.
******************************************************************************************************/
ALTER PROCEDURE [l2].[load_target_obj]
	(
	@tar_obj_id INT,
	@sync_batch_guid UNIQUEIDENTIFIER		
	)
AS
BEGIN
	
	SET NOCOUNT ON;

	/*******************************************/
	----For testing
	--DECLARE 
	--	@tar_obj_id INT,
	--	@sync_batch_guid UNIQUEIDENTIFIER
	--SET @tar_obj_id = 1;
	--SET @sync_batch_guid = NEWID(); 
	/*******************************************/

	DECLARE @tar_db VARCHAR(250), @tar_schema VARCHAR(250), @tar_obj VARCHAR(250), @stg_obj_ref VARCHAR(1000)

	SELECT --TOP 1 
		@tar_db = tar_obj_db,
		@tar_schema = tar_obj_schema,
		@tar_obj = tar_obj,
		@stg_obj_ref = CONCAT(QUOTENAME(T.stg_obj_db),'.',QUOTENAME(T.stg_obj_schema),'.',QUOTENAME(T.stg_obj))
	FROM [l2].[status] T
	WHERE tar_obj_id = @tar_obj_id

	--SELECT @tar_db AS tar_db, @tar_schema AS tar_schema, @tar_obj AS tar_obj, @stg_obj_ref AS stg_obj_ref

	/**************************************************************
	Set sync start dts
	**************************************************************/
	DECLARE @sync_start_dts DATETIME2(7) = SYSDATETIME()
	DECLARE @ParmDefinition nvarchar(500);  
	DECLARE @rowCount INT;  
	DECLARE @sql1 NVARCHAR(4000)

	UPDATE T
	SET sync_status = 1, 	
		sync_batch_guid = NULL,
		sync_start_dts = @sync_start_dts, 
		sync_end_dts = NULL,
		sync_batch_row_count = NULL,
		sync_log_msg = 'Loading...'
	FROM l2.status T
	WHERE 1=1
	AND tar_obj_id = @tar_obj_id;
  
	SET @ParmDefinition = N'@batchGUID UNIQUEIDENTIFIER, @stg_obj_ref VARCHAR(1000), @outputValOut INT OUTPUT';  

	SET @sql1 = 'USE '  + QUOTENAME(DB_NAME())
	SET @sql1 = @sql1 + CHAR(10) + 'EXEC @outputValOut = [l2].[load_' + @tar_schema + '_' + REPLACE(@tar_obj,' ','_') + '] @batchGUID, @stg_obj_ref'
	--PRINT @sql1

	EXECUTE sp_executesql @sql1, @ParmDefinition, @batchGUID = @sync_batch_guid, @stg_obj_ref = @stg_obj_ref, @outputValOut = @rowCount OUTPUT;  
	--PRINT @rowCount;  

	/**************************************************************
	Set sync status
	**************************************************************/
	IF @rowCount >= 0
	BEGIN 
		BEGIN TRY
			DECLARE @p1_params TABLE (sync_batch_dts DATETIME2(7), sync_batch_guid VARCHAR(50))
			DECLARE @sql2 VARCHAR(8000)
			SET @sql2 = 'SELECT TOP 1 etl_stage_to_tar_sync_batch_dts AS sync_batch_dts, etl_stage_to_tar_sync_batch_guid AS sync_batch_guid FROM ' + @stg_obj_ref + ' WITH (NOLOCK) ORDER BY etl_stage_to_tar_sync_batch_dts DESC'
			--PRINT @sql2
			INSERT INTO @p1_params
			EXEC (@sql2)
			--SELECT * FROM @p1_params
		END TRY
		BEGIN CATCH
			INSERT INTO @p1_params VALUES (NULL, NULL)
		END CATCH

		UPDATE T
		SET sync_status = 1, 	
			sync_batch_guid = CASE WHEN @rowCount > 0 THEN (SELECT TOP 1 sync_batch_guid FROM @p1_params) ELSE sync_batch_guid END,
			sync_end_dts = SYSDATETIME(),
			sync_batch_row_count = CASE WHEN @rowCount > 0 THEN @rowCount ELSE 0 END,
			sync_log_msg = 'Success!'
		FROM l2.status T
		WHERE 1=1
		AND tar_obj_id = @tar_obj_id;

		RETURN @rowCount
	END 

	IF @rowCount < 0
	BEGIN 
		UPDATE T
		SET sync_log_msg = 'Failure!'
		FROM l2.status T
		WHERE 1=1
		AND tar_obj_id = @tar_obj_id;

		RETURN @rowCount
	END

END 

/*
DECLARE 
	@tar_obj_id INT,
	@sync_batch_guid UNIQUEIDENTIFIER
SET @tar_obj_id = 1;
SET @sync_batch_guid = NEWID(); 

SELECT * FROM l2.status WHERE tar_obj_id = @tar_obj_id

EXEC [l2].[load_target_obj] @tar_obj_id, @sync_batch_guid

SELECT * FROM l2.status WHERE tar_obj_id = @tar_obj_id

*/