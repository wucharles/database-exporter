package org.db.export.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import org.db.export.common.type.ColumnMetaData;

public interface Exporter {
	/**
	 * The discription about the exporter implementation.<br/>
	 * 
	 * @return
	 */
	String getDescription();
	
	/**
	 * simple description.<br/>
	 * 
	 * @return
	 */
	String getDbName();
	
	/**
	 * the order number,more big more before.<br/>
	 * 
	 * @return
	 */
	int getOrder();
	
	/**
	 * check whether support the special database.<br/>
	 * 
	 * @param databaseProductName
	 * @return
	 */
	boolean isSupport(String databaseProductName);
	
	/**
	 * if no schema configured in common.properties, use jdbc info parse schema info.<br/>
	 * 
	 * @param jdbcUrl
	 * @param userName
	 * @return
	 */
	String getSchemaName(String jdbcUrl, String userName);
	
	/**
	 * return a right catalog if need.<br/>
	 * 
	 * @param catalog
	 * @param schema
	 * @param isCaseSensitive
	 * @return
	 */
	String getCatalog(String catalog, String schema, boolean isDbCaseSensitive);
	
	/**
	 * return a right schema if need.<br/>
	 * 
	 * @param schema
	 * @param isCaseSensitive
	 * @return
	 */
	String getSchema(String schema, boolean isDbCaseSensitive);
	
	/**
	 * Translate to another data type if needed.<br/>
	 * 
	 * @param dataType , reference: {@link java.sql.Types}
	 * @return
	 */
	int filterDataType(int dataType, String typeName);
	
	/**
	 * return the sql to clear all data in specified table.<br/>
	 * 
	 * @param schemaPrefix the schema name, like: "schemaName.";
	 * @param tableName target database table name.
	 * 
	 * @return
	 */
	String genClearTableDML(String schemaPrefix, String tableName);
	
	/**
	 * return the sql to count the number of rows in one table.<br/>
	 * 
	 * @param schemaPrefix schemaPrefix the schema name, like: "schemaName.";
	 * @param tableName source database table name.
	 * @param where count condition, like: "where id > 100"
	 * @return
	 */
	String genCountDML(String schemaPrefix, String tableName, String where);
	
	/**
	 * retuan the sql to fetch a page of data from source database.<br/>
	 * 
	 * <br/>Notes: must support pagination.<br/>
	 * 
	 * @param schemaPrefix
	 * @param tableName
	 * @param where
	 * @return
	 */
	String genSelectDML(String schemaPrefix, String tableName, List<ColumnMetaData> columns, String where);
	
	/**
	 * retuan the sql to fetch a page of data from source database.<br/>
	 * 
	 * <br/>Notes: must support pagination.<br/>
	 * 
	 * @param schemaPrefix
	 * @param tableName
	 * @param where
	 * @return
	 */
	String genInsertDML(String schemaPrefix, String tableName, List<ColumnMetaData> columns);
	
	/**
	 * retuan the sql to fetch a page of data from source database.<br/>
	 * 
	 * <br/>Notes: must support pagination.<br/>
	 * 
	 * @param schemaPrefix
	 * @param tableName
	 * @param where
	 * @return
	 */
	String genUpdateDML(String schemaPrefix, String tableName
			, List<ColumnMetaData> updatedColumns, List<ColumnMetaData> whereColumns);
	
	/**
	 * set pagination parameter from select dml statement.<br/>
	 * 
	 * @param prepared
	 * @param from
	 * @param pageSize
	 */
	void setPaginationParameter(PreparedStatement prepared, long from,
			int pageSize) throws RunnerException;
	
	/**
	 * return the total number updated.<br/>
	 * 
	 * @param nums jdbc result.<br/>
	 * @param srcDataLen the number of batch size.<br/>
	 * 
	 * @return
	 */
	int getBatchExecuteResult(int[] nums, int srcDataLen);
	
	/**
	 * if case-sensitive return ture(MySQL etc.), else return false(Oracle etc.).<br/>
	 * 
	 * @param con
	 * @return
	 */
	boolean isCaseSensitive(Connection con);
}
