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
ALTER PROCEDURE [l1].[load_staging]
AS
BEGIN
	
	SET NOCOUNT ON;	

	DECLARE @ErrorNumber INT, @ErrorMessage NVARCHAR(4000)
	DECLARE @obj_id INT, @server AS VARCHAR(250), @db AS VARCHAR(50), @schema AS VARCHAR(250), @obj AS VARCHAR(250), @sync_enabled AS BIT, @sync_batch_ver VARCHAR(50), @stg_obj_ref VARCHAR(1000)

	DECLARE obj_load_cur CURSOR FOR
    SELECT 
		--TOP 1
		S.src_obj_id, S.src_obj_server, S.src_obj_db, S.src_obj_schema, S.src_obj, S.sync_enabled, QUOTENAME(S.stg_obj_server) + '.' + QUOTENAME(S.stg_obj_db) + '.' + QUOTENAME(S.stg_obj_schema) + '.' + QUOTENAME(S.stg_obj) AS stg_obj_ref
	FROM l1.status S WITH (NOLOCK)
	WHERE 1=1
	AND S.sync_enabled = 1
	ORDER BY S.sync_order	

	OPEN obj_load_cur

	FETCH NEXT FROM obj_load_cur
	INTO @obj_id, @server, @db, @schema, @obj, @sync_enabled, @stg_obj_ref

	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		BEGIN TRY
			
			DECLARE @sync_start_dts DATETIME2(7) = SYSDATETIME()

			SET @sync_batch_ver = (SELECT TOP 1 sync_batch_ver_next FROM l1.status WITH (NOLOCK) WHERE src_obj_id = @obj_id)

			--Load change data
			EXEC l1.load_staging_obj @obj_id
			
			--Force error
			--IF @obj = 'contents' 
			--	SELECT 1/0

		END TRY
			
		BEGIN CATCH
			
			SET @ErrorNumber = ERROR_NUMBER();
			SET @ErrorMessage = ERROR_MESSAGE();

			UPDATE so
			SET sync_status = CASE WHEN sync_status < 1 THEN sync_status - 1 ELSE -1 END, 				
				sync_batch_dts = NULL,
				sync_batch_ver = NULL,
				--sync_batch_ver_next = NULL,
				sync_start_dts = @sync_start_dts,
				sync_end_dts = SYSDATETIME(),
				sync_batch_row_count = NULL,
				sync_log_msg = 'Failed!!! Error Msg : ' + LEFT(@ErrorMessage,1000)
			FROM l1.status so
			WHERE 1=1
			AND src_obj_id = @obj_id;

			--Email notification
			DECLARE @dbMailAccount AS VARCHAR(25), @recipientsOnFailure AS VARCHAR(MAX), @subject AS VARCHAR(1000), @body AS VARCHAR(4000)
	
			SET @dbMailAccount = 'DBA'
			SET @recipientsOnFailure = 'emailReceipent@email.com'
			SET @subject =  @stg_obj_ref + ' : Data Load Failure! Please investigate.'

			SET @body = 
				'LogDTS : ' + CAST(GETDATE() AS VARCHAR(50)) + CHAR(10) + 
				'Error Number : ' + CAST(@ErrorNumber AS VARCHAR(50)) + CHAR(10) +
				'Error Message : ' + @ErrorMessage

			EXEC msdb..sp_send_dbmail 
				@profile_name = @dbMailAccount,
				@recipients = @recipientsOnFailure,
				@subject = @subject,
				@importance = 'High',
				@body = @body

		END CATCH
			
		FETCH NEXT FROM obj_load_cur
		INTO @obj_id, @server, @db, @schema, @obj, @sync_enabled, @stg_obj_ref
			
	END

	CLOSE obj_load_cur
	DEALLOCATE obj_load_cur

END

/*

select * FROM l1.status S WITH (NOLOCK)

EXEC l1.load_staging
*/
