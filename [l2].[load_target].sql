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
ALTER PROCEDURE [l2].[load_target] (@loadBatch VARCHAR(50) = NULL)
AS
BEGIN
	SET NOCOUNT ON;	

	IF @loadBatch IS NULL
	BEGIN 
		PRINT 'Error! @loadBatch parameter cannot be NULL'
		RETURN -1
	END
		
	DECLARE @ErrorNumber INT, @ErrorMessage NVARCHAR(4000)
	DECLARE @tar_obj_id INT, @tar_db AS VARCHAR(250), @tar_schema AS VARCHAR(250), @tar_obj AS VARCHAR(250), @tar_obj_ref VARCHAR(1000), @stg_db AS VARCHAR(250), @stg_schema AS VARCHAR(250), @stg_obj AS VARCHAR(250), @stg_obj_ref VARCHAR(1000)
	DECLARE @sync_batch_guid UNIQUEIDENTIFIER
	DECLARE @sql_guid_update VARCHAR(4000), @sql_guid_delete VARCHAR(4000), @sql_guid_update_rowCount INT 

	DECLARE @ErrorNumber1 INT, @ErrorMessage1 NVARCHAR(4000)
	DECLARE @sync_start_dts DATETIME2(7)

	/***********************************************
	Iterate through tar obj
	***********************************************/
	DECLARE tar_load_cur CURSOR FOR
    SELECT --TOP 1 
		S.tar_obj_id, 
		S.tar_obj_db, S.tar_obj_schema, S.tar_obj, CONCAT(QUOTENAME(S.tar_obj_db),'.',QUOTENAME(S.tar_obj_schema),'.',QUOTENAME(S.tar_obj)) AS tar_ref,
		S.stg_obj_db, S.stg_obj_schema, S.stg_obj, CONCAT(QUOTENAME(S.stg_obj_db),'.',QUOTENAME(S.stg_obj_schema),'.',QUOTENAME(S.stg_obj)) AS stg_ref
	FROM l2.status S
	WHERE 1=1
	AND S.sync_enabled = 1
	AND S.sync_batch = @loadBatch
	ORDER BY tar_obj_id
	
	OPEN tar_load_cur

	FETCH NEXT FROM tar_load_cur
	INTO @tar_obj_id, @tar_db, @tar_schema, @tar_obj, @tar_obj_ref, @stg_db, @stg_schema, @stg_obj, @stg_obj_ref

	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		BEGIN TRY

			/***********************************************
			Set process GUID for stage data
			***********************************************/
			SET @sync_batch_guid = NEWID()
			SET @sql_guid_update = 'WITH CTE AS (SELECT TOP (100000) * FROM ' + @stg_obj_ref + ' WHERE [etl_stage_to_tar_sync_batch_guid] IS NULL ORDER BY [etl_src_to_stage_sync_batch_dts])'
			SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'UPDATE CTE'
			SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'SET [etl_stage_to_tar_sync_batch_dts] = SYSDATETIME(), [etl_stage_to_tar_sync_batch_guid] = ''' + CAST(@sync_batch_guid AS VARCHAR(50)) + ''''
				
			--PRINT @sql_guid_update
			BEGIN TRANSACTION setBatchGUID
				EXEC (@sql_guid_update)
				SET @sql_guid_update_rowCount = @@ROWCOUNT
			COMMIT TRANSACTION setBatchGUID

			IF @sql_guid_update_rowCount = 0
			BEGIN 
				PRINT CONCAT('No records to process! ', @stg_obj_ref, ' : skipped.')
				GOTO CONT
			END 
				
			PRINT CONCAT('Success! ', @stg_obj_ref, ' : [etl_stage_to_tar_sync_batch_guid] = ', CAST(@sync_batch_guid AS VARCHAR(50)),' successfully set.')

			BEGIN TRY
			
				SET @sync_start_dts = SYSDATETIME()
					
				--BEGIN TRANSACTION importIntoTar
				--Load data from stage
				DECLARE @returnVal INT 
				--SELECT @tar_obj_id AS tar_obj_id, @sync_batch_guid AS sync_batch_guid
				EXEC @returnVal = [l2].[load_target_obj] @tar_obj_id, @sync_batch_guid

				--COMMIT TRANSACTION importIntoTar
				PRINT CONCAT('Success! ',@stg_obj_ref, ' -> ',@tar_obj_ref)

			END TRY
			
			BEGIN CATCH
			
				--IF (@@TRANCOUNT > 0)
				--BEGIN
				--	ROLLBACK TRANSACTION importIntoTar
				--END

				SET @ErrorNumber1 = ERROR_NUMBER();
				SET @ErrorMessage1 = ERROR_MESSAGE();

				PRINT CONCAT('Error! ', @stg_obj_ref, ' -> ', @tar_obj_ref,' : Error Number: ', CAST(@ErrorNumber1 AS VARCHAR(10)), ': ', LEFT(@ErrorMessage1,1000))

				UPDATE so
				SET sync_status = CASE WHEN sync_status < 1 THEN sync_status - 1 ELSE -1 END, 				
					sync_batch_guid = NULL,
					sync_start_dts = @sync_start_dts,
					sync_end_dts = SYSDATETIME(),
					sync_batch_row_count = NULL,
					sync_log_msg = 'Failed!!! Error Msg : ' + LEFT(@ErrorMessage1,1000)
				FROM l2.status so
				WHERE 1=1
				AND tar_obj_id = @tar_obj_id;

				/***********************************************
				Set process GUID back to NULL in case of error
				***********************************************/
				SET @sql_guid_update = 'UPDATE ' + @stg_obj_ref 
				SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'SET [etl_stage_to_tar_sync_batch_dts] = NULL, [etl_stage_to_tar_sync_batch_guid] = NULL'
				SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'WHERE [etl_stage_to_tar_sync_batch_guid] = ''' + CAST(@sync_batch_guid AS VARCHAR(50)) + ''''

				--PRINT @sql_guid_update
				BEGIN TRANSACTION nullBatchGUID
					EXEC (@sql_guid_update)
				COMMIT TRANSACTION nullBatchGUID
				PRINT CONCAT('Reverted back GUID assignement! ', @stg_obj_ref, ' : [etl_stage_to_tar_sync_batch_guid] = ', CAST(@sync_batch_guid AS VARCHAR(50)),' successfully set back to NULL.')

			END CATCH

			--If return code is -ve then undo
			IF @returnVal < 0
			BEGIN 


				SET @ErrorNumber1 = ERROR_NUMBER();
				SET @ErrorMessage1 = ERROR_MESSAGE();

				PRINT CONCAT('Error! ', @stg_obj_ref, ' -> ', @tar_obj_ref,' : Error Number: ', CAST(@ErrorNumber1 AS VARCHAR(10)), ': ', LEFT(@ErrorMessage1,1000))

				UPDATE so
				SET sync_status = CASE WHEN sync_status < 1 THEN sync_status - 1 ELSE -1 END, 				
					sync_batch_guid = NULL,
					sync_start_dts = @sync_start_dts,
					sync_end_dts = SYSDATETIME(),
					sync_batch_row_count = NULL,
					sync_log_msg = 'Failed!!! Error Msg : ' + LEFT(@ErrorMessage1,1000)
				FROM l2.status so
				WHERE 1=1
				AND tar_obj_id = @tar_obj_id;

				/***********************************************
				Set process GUID back to NULL in case of error
				***********************************************/
				SET @sql_guid_update = 'UPDATE ' + @stg_obj_ref 
				SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'SET [etl_stage_to_tar_sync_batch_dts] = NULL, [etl_stage_to_tar_sync_batch_guid] = NULL'
				SET @sql_guid_update = @sql_guid_update + CHAR(10) + 'WHERE [etl_stage_to_tar_sync_batch_guid] = ''' + CAST(@sync_batch_guid AS VARCHAR(50)) + ''''

				--PRINT @sql_guid_update
				BEGIN TRANSACTION nullBatchGUID
					EXEC (@sql_guid_update)
				COMMIT TRANSACTION nullBatchGUID
				PRINT CONCAT('Reverted back GUID assignement! ', @stg_obj_ref, ' : [etl_stage_to_tar_sync_batch_guid] = ', CAST(@sync_batch_guid AS VARCHAR(50)),' successfully set back to NULL.')

			END

			--SELECT SYSDATETIME()
			/***********************************************
			Delete processed data from stage
			***********************************************/
			BEGIN 

				DECLARE @stageRetentionHours INT
				SET @stageRetentionHours = 3*24		--Default is 3 days

				--Enable below to delete processed data after 3 days
				SET @sql_guid_delete = 'DELETE FROM ' + @stg_obj_ref			
				SET @sql_guid_delete = @sql_guid_delete + CHAR(10) + 'WHERE DATEDIFF(HOUR,[etl_stage_to_tar_sync_batch_dts],SYSDATETIME()) > ' + CAST(@stageRetentionHours AS varchar(5)) + ' AND [etl_stage_to_tar_sync_batch_guid] IS NOT NULL'

				--PRINT @@sql_guid_delete
				EXEC (@sql_guid_delete)
				PRINT CONCAT('Success! ', @stg_obj_ref, ' : records with [etl_stage_to_tar_sync_batch_dts] > ', CAST(@stageRetentionHours AS varchar(5)), ' hours successfully deleted.')

			END 

		END TRY
			
		BEGIN CATCH
			
			IF (@@TRANCOUNT > 0)
			BEGIN
				ROLLBACK TRANSACTION setBatchGUID
				ROLLBACK TRANSACTION nullBatchGUID
			END 
			SET @ErrorNumber = ERROR_NUMBER();
			SET @ErrorMessage = ERROR_MESSAGE();

			PRINT CONCAT('Error! ', @stg_obj_ref, ' : Error Number: ', CAST(@ErrorNumber AS VARCHAR(10)), ': ', LEFT(@ErrorMessage,1000))
			--RETURN

		END CATCH
		
		CONT:
		FETCH NEXT FROM tar_load_cur
		INTO @tar_obj_id, @tar_db, @tar_schema, @tar_obj, @tar_obj_ref, @stg_db, @stg_schema, @stg_obj, @stg_obj_ref
			
	END

	CLOSE tar_load_cur
	DEALLOCATE tar_load_cur

END

/*
DECLARE @loadBatch VARCHAR(50) = 'Contents'

SELECT * FROM l2.status WHERE sync_batch = @loadBatch

EXEC [l2].[load_target] @loadBatch
*/
