package org.db.export.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.db.export.common.AbstractExporter;
import org.db.export.common.Exporter;
import org.db.export.common.RunnerException;
import org.db.export.common.type.ColumnMetaData;
import org.db.export.common.type.MetaDataStatus;
import org.db.export.common.util.DataSourceUtils;

public class DataExporter extends AbstractExporter implements Exporter {
	
	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public boolean isSupport(String databaseProductName) {
		return databaseProductName != null
				&& databaseProductName.toLowerCase().indexOf("mysql") >= 0;
	}

	@Override
	public String getSchemaName(String jdbcUrl, String userName) {
		String schema = null;
		if(jdbcUrl != null && jdbcUrl.length() > 0) {
			int index = jdbcUrl.lastIndexOf('/');
			int endIndex = jdbcUrl.lastIndexOf('?');
			if(endIndex <= 0) {
				endIndex = jdbcUrl.length();
			}
			
			if(index >= 0) {
				schema = jdbcUrl.substring(index + 1, endIndex);
			}
		}
		return schema;
	}

	@Override
	public String getDescription() {
		return "Exporter for MySQL database.";
	}

	@Override
	public String getCatalog(String catalog, String schema,
			boolean isDbCaseSensitive) {
		if(catalog == null) {
			catalog = schema;
		}
		
		if(catalog == null) {
			return null;
		}
		
		return isDbCaseSensitive ? catalog : catalog.toUpperCase() ;
	}

	@Override
	public String getSchema(String schema, boolean isDbCaseSensitive) {
		return isDbCaseSensitive ? schema : schema.toUpperCase();
	}

	@Override
	public int filterDataType(int dataType, String typeName) {
		return dataType;
	}

	@Override
	public String getDbName() {
		return "MySQL";
	}

	@Override
	public String genSelectDML(String schemaPrefix, String tableName,
			List<ColumnMetaData> columns, String where) {
		StringBuilder select = new StringBuilder();
		select.append("SELECT ");
		
		StringBuilder colsSb = new StringBuilder();
		for(ColumnMetaData col : columns) {
			if(col.getStatus() != MetaDataStatus.NORMAL) {
				continue;
			}
			
			colsSb.append(col.getName()).append(",");
		}
		colsSb.setLength(colsSb.length() - 1);
		
		select.append(colsSb).append(" FROM ")
			  .append(schemaPrefix)
			  .append(tableName);
		select.append(" ").append(where);
		select.append(" LIMIT  ?,?");
		
		return select.toString();
	}

	@Override
	public void setPaginationParameter(PreparedStatement prepared, long from,
			int pageSize) throws RunnerException{
		try {
			prepared.setLong(1, from - 1);
			prepared.setInt(2, pageSize);
		} catch (SQLException e) {
			new RunnerException("set pagination parameter error.", e);
		}
	}

	/**
	 * mysql support batch result.
	 * 
	 */
	@Override
	public int getBatchExecuteResult(int[] nums, int srcDataLen) {
		int total = 0;
		for(int num : nums) {
			total += num < 0 ? 0 : num;
		}
		return total;
	}
	
	@Override
	public boolean isCaseSensitive(Connection con) {
		String sql = "show variables like 'lower_case_table_names'";
		
		Statement stat = null;
		ResultSet rs = null;
		try {
			stat = con.createStatement();
			rs = stat.executeQuery(sql);
			if(rs.next()) {
				int cs = rs.getInt("VALUE");
				
				return cs == 1 ? false : true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceUtils.closeQuietly(rs);
			DataSourceUtils.closeQuietly(stat);
			DataSourceUtils.closeQuietly(con);
		}
		
		return true;
	}
}
