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
ALTER PROCEDURE [l1].[load_staging_obj]
	(
	@src_obj_id INT
	)
AS
BEGIN
	
	SET NOCOUNT ON;	

	/*******************************************/
	--For testing
	--DECLARE @src_obj_id INT = 1
	/*******************************************/
	
	DECLARE @src_server VARCHAR(250), @src_db VARCHAR(250), @src_schema VARCHAR(250), @src_obj VARCHAR(250)
	DECLARE @stg_server VARCHAR(250), @stg_db VARCHAR(250), @stg_schema VARCHAR(250), @stg_obj VARCHAR(250), @sync_batch_ver VARCHAR(50);

	SELECT TOP 1 
		@src_server = src_obj_server,
		@src_db = src_obj_db,
		@src_schema = src_obj_schema,
		@src_obj = src_obj,
		@stg_server = stg_obj_server,
		@stg_db = stg_obj_db,
		@stg_schema = stg_obj_schema,
		@stg_obj = stg_obj, 
		@sync_batch_ver = sync_batch_ver
	FROM [l1].[status]
	WHERE src_obj_id = @src_obj_id
		
	--SELECT @src_server AS src_server, @src_db AS src_db, @src_schema AS src_schema, @src_obj AS src_obj, @stg_server AS stg_server, @stg_db AS stg_db, @stg_schema AS stg_schema, @stg_obj AS stg_obj, @sync_batch_ver AS sync_batch_ver

	DECLARE @tarObj VARCHAR(500), @srcObj VARCHAR(500), @colHeaderSelect VARCHAR(MAX), @colHeaderSelectSQL VARCHAR(MAX), @colHeaderInsert VARCHAR(MAX), @colHeaderInsertSQL VARCHAR(MAX), @joinPredicateClause VARCHAR(1000), @joinPredicateClauseSQL VARCHAR(MAX)
	DECLARE @sql1 VARCHAR(MAX)
	DECLARE @paramOutput TABLE (varName VARCHAR(50), varValue VARCHAR(8000))
	
	/**************************************************************
	Set sync start dts
	**************************************************************/
	DECLARE @sync_start_dts DATETIME2(7) = SYSDATETIME()

	/**************************************************************
	Build select, insert and join clause dynamically
	**************************************************************/
	SET @srcObj = QUOTENAME(@src_db) + '.' + QUOTENAME(@src_schema) + '.' + QUOTENAME(@src_obj)
	SET @tarObj = QUOTENAME(@stg_db) + '.' + QUOTENAME(@stg_schema) + '.' + QUOTENAME(@stg_obj)	

	DECLARE @msgToConsole VARCHAR(1000)
	SET @msgToConsole = CONCAT('Importing delta : ',@srcObj,' -> ',@tarObj)
	RAISERROR (@msgToConsole, 0, 1) WITH NOWAIT

	SET @colHeaderInsertSQL = 
		'SELECT ''@colHeaderInsert'', SUBSTRING((
			SELECT '', '' + QUOTENAME(COLUMN_NAME)
			FROM ' + @stg_db +'.INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)
			WHERE 1=1
			AND TABLE_CATALOG = ''' + @stg_db + '''
			AND TABLE_SCHEMA = ''' + @stg_schema + '''
			AND TABLE_NAME = ''' + @stg_obj + '''
			ORDER BY ORDINAL_POSITION
			FOR XML PATH ('''')			
		), 3, 8000)'
	--PRINT @colHeaderInsertSQL
	INSERT INTO @paramOutput
	EXEC (@colHeaderInsertSQL) 
	SET @colHeaderInsert = (SELECT TOP 1 varValue FROM @paramOutput WHERE varName = '@colHeaderInsert')
	--PRINT @colHeaderInsert

	SET @colHeaderSelectSQL = 
		'SELECT ''@colHeaderSelect'', SUBSTRING((
			SELECT CASE WHEN PK.PrimaryKey = 1 THEN '', ct.'' ELSE '', src.'' END + QUOTENAME(COLUMN_NAME)
			FROM ' + @stg_db + '.INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)
			OUTER APPLY (SELECT TOP 1 L1.PrimaryKey FROM ' + QUOTENAME(@stg_db) + '.L1.src_objects_columns L1 WITH (NOLOCK) WHERE L1.src_db = ''' + @src_db + ''' AND L1.src_schema = ''' + @src_schema + ''' AND L1.src_obj = ''' + @src_obj + ''' AND L1.src_column_name = COLUMN_NAME) PK
			WHERE 1=1
			AND TABLE_CATALOG = ''' + @stg_db + '''
			AND TABLE_SCHEMA = ''' + @stg_schema + '''
			AND TABLE_NAME = ''' + @stg_obj + '''
			ORDER BY ORDINAL_POSITION
			FOR XML PATH ('''')			
		), 3, 8000)'
	--PRINT @colHeaderSelectSQL
	INSERT INTO @paramOutput
	EXEC (@colHeaderSelectSQL)
	SET @colHeaderSelect = (SELECT TOP 1 varValue FROM @paramOutput WHERE varName = '@colHeaderSelect')
	--PRINT @colHeaderSelect

	--Set values for metadata fields
	SET @colHeaderSelect = REPLACE(@colHeaderSelect, 'src.[etl_src_to_stage_sync_batch_dts]', 'SYSDATETIME() AS [etl_src_to_stage_sync_batch_dts]')
	SET @colHeaderSelect = REPLACE(@colHeaderSelect, 'src.[etl_src_to_stage_sync_batch_ver_next]', '@currentCTVersion AS [etl_src_to_stage_sync_batch_ver_next]')
	SET @colHeaderSelect = REPLACE(@colHeaderSelect, 'src.[etl_src_to_stage_sync_chg_op]', 'ct.SYS_CHANGE_OPERATION AS [etl_src_to_stage_sync_chg_op]')
	SET @colHeaderSelect = REPLACE(@colHeaderSelect, 'src.[etl_stage_to_tar_sync_batch_dts]', 'NULL AS [etl_stage_to_tar_sync_batch_dts]')
	SET @colHeaderSelect = REPLACE(@colHeaderSelect, 'src.[etl_stage_to_tar_sync_batch_guid]', 'NULL AS [etl_stage_to_tar_sync_batch_guid]')

	SET @joinPredicateClauseSQL = 
		'SELECT ''@joinPredicateClause'', SUBSTRING((
			SELECT 
				'' AND src.'' + QUOTENAME(src_column_name) + '' = '' + ''ct.'' + QUOTENAME(src_column_name)
			FROM l1.src_objects_columns WITH (NOLOCK)
			WHERE 1=1
			AND src_db = ''' + @src_db + '''
			AND src_schema = ''' + @src_schema + '''
			AND src_obj = ''' + @src_obj + '''
			AND PrimaryKey = 1
			FOR XML PATH ('''')
		),5,8000)'
	--PRINT @joinPredicateClauseSQL
	INSERT INTO @paramOutput
	EXEC (@joinPredicateClauseSQL)
	SET @joinPredicateClause = (SELECT TOP 1 varValue FROM @paramOutput WHERE varName = '@joinPredicateClause')
	--PRINT @joinPredicateClause

	/**************************************************************
	Build final data retrival script dynamically
	**************************************************************/	
	SET @sql1 = 'USE '  + QUOTENAME(@src_db)
	SET @sql1 = @sql1 + CHAR(10) + 'DECLARE @lastSycnCTVersion AS BIGINT, @currentCTVersion AS BIGINT' 
	SET @sql1 = @sql1 + CHAR(10) + 'SET @currentCTVersion = CHANGE_TRACKING_CURRENT_VERSION()'
	SET @sql1 = @sql1 + CHAR(10) + 'SET @lastSycnCTVersion = ' + CASE WHEN @sync_batch_ver IS NOT NULL THEN @sync_batch_ver ELSE '(SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(''' + @srcObj + ''')))' END 
	
	SET @sql1 = @sql1 + CHAR(10) + 'INSERT INTO ' + @tarObj + ' (' + @colHeaderInsert + ')'
	SET @sql1 = @sql1 + CHAR(10) + 'SELECT ' + @colHeaderSelect
	SET @sql1 = @sql1 + CHAR(10) + 'FROM ' + @srcObj + ' AS src WITH (NOLOCK)'
	SET @sql1 = @sql1 + CHAR(10) + 'RIGHT JOIN CHANGETABLE(CHANGES ' + @srcObj + ', @lastSycnCTVersion) AS ct ON ' + @joinPredicateClause

	--SET @sql1 = @sql1 + CHAR(10) + 'OPTION (RECOMPILE)'
	--PRINT @sql1

	/**************************************************************
	Read change data from appropraite linked server
	**************************************************************/
	IF @src_db IN ('PH_PROD_VEGAS','PH_PROD','PH_PROD_KC')
		BEGIN
			EXEC (@sql1) AT [PH-LIS]
		END
		
	DECLARE @rowCount INT
	SET @rowCount = ISNULL(@@ROWCOUNT,0)
	PRINT @rowCount
	/**************************************************************
	Set sync status
	**************************************************************/
	IF @rowCount > 0
	BEGIN 
		DECLARE @p1_params TABLE (sync_batch_dts DATETIME2(7), sync_batch_ver_next VARCHAR(50))
		DECLARE @sql2 VARCHAR(8000)
		SET @sql2 = 'SELECT TOP 1 etl_src_to_stage_sync_batch_dts AS sync_batch_dts, etl_src_to_stage_sync_batch_ver_next AS sync_batch_ver_next FROM ' + @tarObj + ' WITH (NOLOCK) ORDER BY etl_src_to_stage_sync_batch_dts DESC'
		--PRINT @sql2
		INSERT INTO @p1_params
		EXEC (@sql2)
		--SELECT * FROM @p1_params
	END

	UPDATE T
	SET sync_status = 1, 
		sync_batch_dts = CASE WHEN @rowCount > 0 THEN (SELECT TOP 1 sync_batch_dts FROM @p1_params) ELSE sync_batch_dts END,
		sync_batch_ver = CASE WHEN @rowCount > 0 THEN sync_batch_ver_next ELSE sync_batch_ver END,
		sync_batch_ver_next = CASE WHEN @rowCount > 0 THEN (SELECT TOP 1 sync_batch_ver_next FROM @p1_params) ELSE sync_batch_ver_next END,
		sync_start_dts = @sync_start_dts, 
		sync_end_dts = SYSDATETIME(),
		sync_batch_row_count = CASE WHEN @rowCount > 0 THEN @rowCount ELSE 0 END,
		sync_log_msg = 'Success!'
	FROM l1.status T
	WHERE T.src_obj_id = @src_obj_id

END 

/*

DECLARE @src_obj_id INT = 3

SELECT * FROM l1.status WHERE src_obj_id = @src_obj_id

EXEC [l1].[load_staging_obj] @src_obj_id
*/