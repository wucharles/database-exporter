package org.db.export.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.db.export.common.AbstractExporter;
import org.db.export.common.Exporter;
import org.db.export.common.RunnerException;
import org.db.export.common.type.ColumnMetaData;
import org.db.export.common.type.MetaDataStatus;

public class DataExporter extends AbstractExporter implements Exporter {

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public boolean isSupport(String databaseProductName) {
		return databaseProductName != null
				&& databaseProductName.toLowerCase().indexOf("oracle") >= 0;
	}

	@Override
	public String getSchemaName(String jdbcUrl, String userName) {
		return userName.toUpperCase();
	}

	@Override
	public String getDescription() {
		return "Exporter for Oracle database.";
	}

	@Override
	public String getCatalog(String catalog, String schema,
			boolean isDbCaseSensitive) {
		return null;
	}

	@Override
	public String getSchema(String schema, boolean isDbCaseSensitive) {
		return isDbCaseSensitive ? schema : schema.toUpperCase();
	}

	@Override
	public int filterDataType(int dataType, String typeName) {
		if(typeName.equalsIgnoreCase("NCLOB")) {
			return Types.NCLOB;
		}
		
		return dataType;
	}

	@Override
	public String getDbName() {
		return "Oracle";
	}

	@Override
	public String genSelectDML(String schemaPrefix, String tableName,
			List<ColumnMetaData> columns, String where) {
		StringBuilder inner = new StringBuilder();
		inner.append("SELECT ");
		
		StringBuilder colsSb = new StringBuilder();
		for(ColumnMetaData col : columns) {
			if(col.getStatus() != MetaDataStatus.NORMAL) {
				continue;
			}
			
			colsSb.append(col.getName()).append(",");
		}
		colsSb.setLength(colsSb.length() - 1);
		
		inner.append(colsSb).append(" FROM ").append(schemaPrefix).append(tableName);
		inner.append(" ").append(where);
		
		//row num
		StringBuilder rownum = new StringBuilder();
		rownum.append("SELECT ").append(colsSb).append(", rownum row_num ");
		rownum.append(" FROM (").append(inner).append(")");
		
		//result
		StringBuilder select = new StringBuilder();
		select.append("SELECT ").append(colsSb).append(" FROM (");
		select.append(rownum).append(")");
		select.append(" WHERE row_num >= ? AND row_num < ?");
		
		return select.toString();
	}

	@Override
	public void setPaginationParameter(PreparedStatement prepared, long from,
			int pageSize)  throws RunnerException{
		try {
			prepared.setLong(1, from);
			prepared.setLong(2, from + pageSize);
		} catch (SQLException e) {
			new RunnerException("set pagination parameter error.", e);
		}
	}

	/**
	 * Oracle don't support batch result.<br/>
	 * 
	 */
	@Override
	public int getBatchExecuteResult(int[] nums, int srcDataLen) {
		return super.getBatchExecuteResult(nums, srcDataLen);
	}
}
