package org.db.export.common;

import java.sql.Connection;
import java.util.List;

import org.db.export.common.type.ColumnMetaData;
import org.db.export.common.type.MetaDataStatus;

public abstract class AbstractExporter implements Exporter {
	@Override
	public String genClearTableDML(String schemaPrefix, String tableName) {
		return "DELETE FROM "  + schemaPrefix + tableName;
	}

	@Override
	public String genCountDML(String schemaPrefix, String tableName, String where) {
		return "SELECT COUNT(1) FROM " +  schemaPrefix + tableName + " " + where;
	}

	@Override
	public String genInsertDML(String schemaPrefix, String tableName,
			List<ColumnMetaData> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(schemaPrefix)
		.append(tableName).append("(");

		for(ColumnMetaData col : columns) {
			if(col.getStatus() != MetaDataStatus.NORMAL) {
				continue;
			}

			sb.append(col.getName()).append(",");
		}
		sb.setLength(sb.length() - 1);
		sb.append(") VALUES (");

		for(ColumnMetaData col : columns) {
			if(col.getStatus() == MetaDataStatus.NORMAL) {
				sb.append("?,");
			}
		}
		sb.setLength(sb.length() - 1);
		sb.append(")");

		return sb.toString();
	}
	
	@Override
	public String genUpdateDML(String schemaPrefix, String tableName
			, List<ColumnMetaData> updatedColumns, List<ColumnMetaData> whereColumns) {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ").append(schemaPrefix)
		  .append(tableName).append(" SET ");
		
		for(ColumnMetaData col : updatedColumns) {
			if(col.getStatus() != MetaDataStatus.NORMAL) {
				continue;
			}
			
			sb.append(col.getName()).append(" = ?,");
		}
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE ");
		
		//TODO: update key cols, exclude lob's;
		for(ColumnMetaData col : whereColumns) {
			if(col.getStatus() == MetaDataStatus.NORMAL) {
				sb.append(col.getName()).append("=? AND ");
			}
		}
		sb.setLength(sb.length() - 5);
		
		return sb.toString();
	}
	
	@Override
	public int getBatchExecuteResult(int[] nums, int srcDataLen) {
		return srcDataLen;
	}

	@Override
	public boolean isCaseSensitive(Connection con) {
		return false;
	}
}
