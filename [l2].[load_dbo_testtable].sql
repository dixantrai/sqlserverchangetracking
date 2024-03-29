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
ALTER PROCEDURE [l2].[load_dbo_testtable]
(@batchGUID UNIQUEIDENTIFIER, @stg_obj_ref VARCHAR(1000) = NULL) AS
BEGIN
	
	SET NOCOUNT ON;

	IF @batchGUID IS NULL OR @stg_obj_ref IS NULL
		RETURN -1

	BEGIN TRY
		IF @stg_obj_ref = '[stageDB].[dbo].[testtable]'
		BEGIN
			
			DECLARE @rowCount INT = 0, @finalRowCount INT = 0

			-- D records
			BEGIN
				DELETE T 
				FROM [stageDB].[dbo].[testtable] T
				JOIN 
					(				
					SELECT [testtable_pk]
					FROM 
						(
						SELECT [testtable_pk], etl_src_to_stage_sync_chg_op
							, ROW_NUMBER() OVER (PARTITION BY [testtable_pk] ORDER BY S.etl_src_to_stage_sync_batch_dts DESC) R
						FROM [stage].[contents_1lee] S
						WHERE 1=1
						AND S.etl_stage_to_tar_sync_batch_guid = @batchGUID
						) S
					WHERE 1=1
					AND R = 1
					AND S.etl_src_to_stage_sync_chg_op = 'D'
					) S
					ON T.[testtable_pk] = S.[testtable_pk]

				SET @rowCount = ISNULL(@@ROWCOUNT,0)
				SET @finalRowCount = @finalRowCount + @rowCount
				RAISERROR ('	Deleted %i records.', 0, 1, @rowCount) WITH NOWAIT
				SET @rowCount = 0
			END
			
			-- U records
			BEGIN 

				DELETE T
				FROM [stageDB].[dbo].[testtable] T
				JOIN 
					(				
					SELECT [testtable_pk]
					FROM
						(
						SELECT [testtable_pk],[col1]
							, ROW_NUMBER() OVER (PARTITION BY [testtable_pk] ORDER BY S.etl_src_to_stage_sync_batch_dts DESC) R
						FROM [stage].[testtable] S
						WHERE 1=1
						AND S.etl_stage_to_tar_sync_batch_guid = @batchGUID
						) S
					WHERE 1=1
					AND R = 1
					AND S.etl_src_to_stage_sync_chg_op = 'U'
					) S
					ON T.[testtable_pk] = S.[testtable_pk]
			
				INSERT INTO [stageDB].[dbo].[testtable] ([testtable_pk],[col1])
				SELECT [testtable_pk],[col1]
				FROM 
					(
					SELECT *
						, ROW_NUMBER() OVER (PARTITION BY [testtable_pk] ORDER BY S.etl_src_to_stage_sync_batch_dts DESC) R
					FROM [stage].[testtable] S
					WHERE 1=1
					AND S.etl_stage_to_tar_sync_batch_guid = @batchGUID
					) S
				WHERE 1=1
				AND R = 1
				AND S.etl_src_to_stage_sync_chg_op = 'U'
				AND NOT EXISTS (SELECT [testtable_pk] FROM [stageDB].[dbo].[testtable] WHERE [testtable_pk]= S.[testtable_pk])

				SET @rowCount = ISNULL(@@ROWCOUNT,0)
				SET @finalRowCount = @finalRowCount + @rowCount
				RAISERROR ('	Updated %i records.', 0, 1, @rowCount) WITH NOWAIT
				SET @rowCount = 0
			END

			-- I records
			BEGIN 
				INSERT INTO [stageDB].[dbo].[testtable] ([testtable_pk],[col1])
				SELECT [testtable_pk],[col1]
				FROM 
					(
					SELECT *
						, ROW_NUMBER() OVER (PARTITION BY [testtable_pk] ORDER BY S.etl_src_to_stage_sync_batch_dts DESC) R
					FROM [stage].[contents_1lee] S
					WHERE 1=1
					AND S.etl_stage_to_tar_sync_batch_guid = @batchGUID
					) S
				WHERE 1=1
				AND R = 1
				AND S.etl_src_to_stage_sync_chg_op = 'I'
				AND NOT EXISTS (SELECT [testtable_pk] FROM [stageDB].[dbo].[testtable] WHERE [testtable_pk]= S.[testtable_pk])

				SET @rowCount = ISNULL(@@ROWCOUNT,0)
				SET @finalRowCount = @finalRowCount + @rowCount
				RAISERROR ('	Inserted %i records.', 0, 1, @rowCount) WITH NOWAIT
				SET @rowCount = 0
			END
			
			RETURN @finalRowCount
			
		END
	END TRY

	BEGIN CATCH

		DECLARE @errorNumber INT, @errorMessage NVARCHAR(4000)
		SET @errorNumber = ERROR_NUMBER();
		SET @errorMessage = ERROR_MESSAGE();

		INSERT INTO [l2].[error_log] (batchGUID, stg_obj_ref, ErrorNumber, ErrorMessage)
		VALUES (@batchGUID, @stg_obj_ref, @errorNumber,@errorMessage)

		RETURN -1

	END CATCH
	
END

/*

DECLARE @batchGUID UNIQUEIDENTIFIER, @stg_obj_ref VARCHAR(1000)
SET @batchGUID = (SELECT TOP 1 etl_stage_to_tar_sync_batch_guid FROM PH_Contents.stage.contents_1lee WHERE etl_stage_to_tar_sync_batch_guid IS NOT NULL) 
SET @stg_obj_ref = '[PH_Contents].[stage].[contents_1lee]'
SELECT @batchGUID, @stg_obj_ref

DECLARE @returnVal INT 
EXEC @returnVal = [l2].[load_dbxx_contents_1lee] @batchGUID, @stg_obj_ref
PRINT @returnVal

SELECT * FROM PH_Contents.stage.contents_1lee WHERE etl_stage_to_tar_sync_batch_guid = @batchGUID
SELECT * FROM PH_Contents.dbxx.contents_1lee
SELECT * FROM [l2].[error_log]

*/



