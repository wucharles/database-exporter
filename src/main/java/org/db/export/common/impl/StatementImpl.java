package org.db.export.common.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.db.export.common.Exporter;
import org.db.export.common.ExporterContext;
import org.db.export.common.RunnerException;
import org.db.export.common.Statement;
import org.db.export.common.type.ColumnMetaData;
import org.db.export.common.type.MetaDataStatus;
import org.db.export.common.util.DataSourceUtils;
import org.db.export.common.util.ProgressUtils;

public class StatementImpl implements Statement{
	
	private String select = null;
	
	private String updateOrInsert = null;
	
	private long from = -1L;
	
	private int size = -1;
	
	private Connection src = null;
	
	private Connection target = null;
	
	private ExporterContext context = null;
	
	private String sourceTable = null;
	
	private String targetTable = null;
	
	public StatementImpl(String sourceTable, String targetTable, String select
			, long from, int size, String updateOrInsert) {
		setFrom(from);
		setSize(size);
		this.setSelect(select);
		this.setUpdateOrInsert(updateOrInsert);
		
		setSourceTable(sourceTable);
		setTargetTable(targetTable);
	}
	
	@Override
	public void bind(Connection src, Connection target) {
		setSrc(src);
		setTarget(target);
	}

	public String getSelect() {
		return select;
	}

	public void setSelect(String select) {
		this.select = select;
	}

	public String getUpdateOrInsert() {
		return updateOrInsert;
	}

	public void setUpdateOrInsert(String updateOrInsert) {
		this.updateOrInsert = updateOrInsert;
	}

	public long getFrom() {
		return from;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public boolean isBinded() {
		return getSrc() != null && getTarget() != null;
	}

	@Override
	public void unbind() {
		DataSourceUtils.closeQuietly(getSrc());
		DataSourceUtils.closeQuietly(getTarget());
	}

	public void setSrc(Connection src) {
		this.src = src;
	}

	public Connection getSrc() {
		return src;
	}

	public void setTarget(Connection target) {
		this.target = target;
	}

	public Connection getTarget() {
		return target;
	}

	@Override
	public void exec() throws RunnerException{
		ResultSet srcTableData = fetchData();
		
		PreparedStatement targetStat = null;
		try {
			targetStat = getTarget().prepareStatement(getUpdateOrInsert());
		} catch (SQLException e) {
			DataSourceUtils.closeQuietly(target);
			DataSourceUtils.closeQuietly(srcTableData);
			try {
				DataSourceUtils.closeQuietly(srcTableData.getStatement());
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			throw new RunnerException("prepare save or insert sql in target database error", e);
		}
		
		try {
			saveOrUpdate(srcTableData,targetStat);
		} catch (SQLException e) {
			throw new RunnerException("execute insert or update in target database error. " +
					"source table = " + getSourceTable() + ", target table = " + getTargetTable()
					+ "\t selectSQL = " + getSelect()
					+ "\t insert/update SQL = " + getUpdateOrInsert(), e);
		} finally {
			DataSourceUtils.closeQuietly(targetStat);
			try {
				DataSourceUtils.closeQuietly(srcTableData.getStatement());
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			DataSourceUtils.closeQuietly(srcTableData);
			DataSourceUtils.commitQuietly(getTarget());
		}
	}

	private void saveOrUpdate(ResultSet srcTableData,
			PreparedStatement targetStat) throws SQLException {
		final Exporter targetExporter = getContext().getTargetExporter();
		Map<String, ColumnMetaData> srcColumnAllMapping = 
			getContext().getSrcTableMetaData().get(sourceTable).getColumnAllMapping();
		List<ColumnMetaData> targetOrderedAllCols =
			getContext().getTargetTableMetaData(sourceTable, targetTable).getOrderedAllCols();
		
		int srcDataLen = 0;
		while(srcTableData.next()) {
			int index = 1;
			++srcDataLen; //Oracle不能批量操作时返回更新条目数
			for(ColumnMetaData col : targetOrderedAllCols){
				if(col.getStatus() != MetaDataStatus.NORMAL) {
					continue;
				}
				
				//如果目标值为null;
				if(srcTableData.getObject(col.getSourceName()) == null) {
					targetStat.setNull(index, col.getType());
					++index;
					continue;
				}
				
				switch(srcColumnAllMapping.get(col.getSourceName()).getType()) {
				case Types.ARRAY:
					//TODO: we need it?
					break;
				case Types.BIGINT:
					targetStat.setLong(index, srcTableData.getLong(col.getSourceName()));
					break;
				case Types.BIT:
					targetStat.setByte(index, srcTableData.getByte(col.getSourceName()));
					break;
				case Types.BLOB:
					targetStat.setBytes(index, srcTableData.getBytes(col.getSourceName()));
					break;
				case Types.BOOLEAN:
					targetStat.setBoolean(index, srcTableData.getBoolean(col.getSourceName()));
					break;
				case Types.CHAR:
					targetStat.setString(index, srcTableData.getString(col.getSourceName()));
					break;
				case Types.CLOB:
					targetStat.setString(index, srcTableData.getString(col.getSourceName()));
					break;
				case Types.DATALINK:
					//TODO: we need it?
					break;
				case Types.DATE:
					targetStat.setDate(index, srcTableData.getDate(col.getSourceName()));
					break;
				case Types.DECIMAL:
					targetStat.setBigDecimal(index, srcTableData.getBigDecimal(col.getSourceName()));
					break;
				case Types.DISTINCT:
					//TODO: we need it?
					break;
				case Types.DOUBLE:
					targetStat.setDouble(index, srcTableData.getDouble(col.getSourceName()));
					break;
				case Types.FLOAT:
					targetStat.setFloat(index, srcTableData.getFloat(col.getSourceName()));
					break;
				case Types.INTEGER:
					targetStat.setInt(index, srcTableData.getInt(col.getSourceName()));
					break;
				case Types.JAVA_OBJECT:
					//TODO: we need it?
					break;
				case Types.LONGNVARCHAR:
					targetStat.setNString(index, srcTableData.getNString(col.getSourceName()));
					break;
				case Types.LONGVARBINARY:
					targetStat.setBytes(index, srcTableData.getBytes(col.getSourceName()));
					break;
				case Types.LONGVARCHAR:
					targetStat.setString(index, srcTableData.getString(col.getSourceName()));
					break;
				case Types.NCHAR:
					targetStat.setNString(index, srcTableData.getNString(col.getSourceName()));
					break;
				case Types.NCLOB:
					targetStat.setNString(index, srcTableData.getNString(col.getSourceName()));
					break;
				case Types.NULL:
					//TODO: we need it?
					break;
				case Types.NUMERIC:
					targetStat.setLong(index, srcTableData.getLong(col.getSourceName()));
					break;
				case Types.NVARCHAR:
					targetStat.setNString(index, srcTableData.getNString(col.getSourceName()));
					break;
				case Types.OTHER:
					//TODO: we need it?
					break;
				case Types.REAL:
					targetStat.setDouble(index, srcTableData.getDouble(col.getSourceName()));
					break;
				case Types.REF:
					//TODO: we need it?
					break;
				case Types.ROWID:
					//TODO: we need it?
					break;
				case Types.SMALLINT:
					targetStat.setShort(index, srcTableData.getShort(col.getSourceName()));
					break;
				case Types.SQLXML:
					//TODO: we need it?
					break;
				case Types.STRUCT:
					//TODO: we need it?
					break;
				case Types.TIME:
					targetStat.setDate(index, srcTableData.getDate(col.getSourceName()));
					break;
				case Types.TIMESTAMP:
					targetStat.setTimestamp(index, srcTableData.getTimestamp(col.getSourceName()));
					break;
				case Types.TINYINT:
					targetStat.setShort(index, srcTableData.getShort(col.getSourceName()));
					break;
				case Types.VARBINARY:
					targetStat.setBytes(index, srcTableData.getBytes(col.getSourceName()));
					break;
				case Types.VARCHAR:
					targetStat.setString(index, srcTableData.getString(col.getSourceName()));
					break;
				default:
					break;
				}
				++index;
			}
			
			targetStat.addBatch();
			targetStat.clearParameters();
		}
		int[] nums = targetStat.executeBatch();
		if(nums == null) {
			return;
		}
		
		int total = targetExporter.getBatchExecuteResult(nums, srcDataLen);
		ProgressUtils.getInstance().inc(sourceTable + "-->" + targetTable, (long)total);
	}

	private ResultSet fetchData() throws RunnerException{
		try {
			PreparedStatement sel = getSrc().prepareStatement(getSelect());
			getContext().getSrcExporter().setPaginationParameter(sel, getFrom(), getSize());
			return sel.executeQuery();
		} catch (SQLException e) {
			throw new RunnerException("execute select data from source database error.", e);
		}
	}

	public void setContext(ExporterContext context) {
		this.context = context;
	}

	public ExporterContext getContext() {
		return context;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}
}
