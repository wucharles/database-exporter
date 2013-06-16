package org.db.export.common.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.db.export.common.util.CommonUtils;

public class TableMetaData {
	private String name = null;
	
	private String sourceName = null; // target table is alias to source table name.
	
	private  MetaDataStatus status = MetaDataStatus.NORMAL;
	
	//all columns as select list in source db, or as insert columns in destination db, or as update columns in destination db.
	private Map<String, ColumnMetaData> columnMapping = null;
	
	private List<ColumnMetaData> orderedCols = null;
	
	//In increment export mode, used as update where condition.
	private Map<String, ColumnMetaData> uniqueMapping = new HashMap<String, ColumnMetaData>(0, 1F);
	
	private List<ColumnMetaData> orderedUniqueCols = new ArrayList<ColumnMetaData>(0);
	
	//all columns in source or destination table.
	private Map<String, ColumnMetaData> columnAllMapping = null;
	
	private List<ColumnMetaData> orderedAllCols = null;
	
	//generally, used to mapping source table column name to target table column name.
	//format: [targetColName, sourceColName]
	private Map<String, String> columnNameMapping = new HashMap<String, String>(0, 1F);
	
	public TableMetaData copy() {
		TableMetaData md = new TableMetaData();
		md.name = this.getName();
		md.sourceName = this.getSourceName();
		md.status = this.getStatus();
		if(CommonUtils.isNotEmpty(this.columnAllMapping)) {
			md.columnAllMapping = new HashMap<String, ColumnMetaData>(this.columnAllMapping.size(), 1F);
			
			for(Entry<String, ColumnMetaData> entry : this.columnAllMapping.entrySet()) {
				md.columnAllMapping.put(entry.getKey(), entry.getValue().copy(md));
			}
			
			//keep order;
			md.orderedAllCols = new ArrayList<ColumnMetaData>(md.columnAllMapping.size());
			for(ColumnMetaData col : this.orderedAllCols) {
				md.orderedAllCols.add(md.columnAllMapping.get(col.getName()));
			}
		}
		
		if(CommonUtils.isNotEmpty(this.columnMapping)) {
			md.columnMapping = new HashMap<String, ColumnMetaData>(this.columnMapping.size(), 1F);
			
			for(Entry<String, ColumnMetaData> entry : this.columnMapping.entrySet()) {
				md.columnMapping.put(entry.getKey(), md.columnAllMapping.get(entry.getKey()));
			}
			
			//keep order;
			md.orderedCols = new ArrayList<ColumnMetaData>(md.columnMapping.size());
			for(ColumnMetaData col : this.orderedCols) {
				md.orderedCols.add(md.columnMapping.get(col.getName()));
			}
		}
		
		if(CommonUtils.isNotEmpty(this.uniqueMapping)) {
			md.uniqueMapping = new HashMap<String, ColumnMetaData>(this.uniqueMapping.size(), 1F);
			
			for(Entry<String, ColumnMetaData> entry : this.uniqueMapping.entrySet()) {
				md.uniqueMapping.put(entry.getKey(), md.columnAllMapping.get(entry.getKey()));
			}
			
			//keep order;
			md.orderedUniqueCols = new ArrayList<ColumnMetaData>(md.uniqueMapping.size());
			for(ColumnMetaData col : this.orderedUniqueCols) {
				md.orderedUniqueCols.add(md.uniqueMapping.get(col.getName()));
			}
		}
		
		if(CommonUtils.isNotEmpty(this.columnNameMapping)) {
			md.columnNameMapping = new HashMap<String, String>(this.columnNameMapping);
		}
		
		return md;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MetaDataStatus getStatus() {
		return status;
	}

	public void setStatus(MetaDataStatus status) {
		this.status = status;
	}

	public Map<String, ColumnMetaData> getColumnMapping() {
		return columnMapping;
	}

	public void setColumnMapping(Map<String, ColumnMetaData> columnMapping) {
		this.columnMapping = columnMapping;
	}

	public List<ColumnMetaData> getOrderedCols() {
		return orderedCols;
	}

	public void setOrderedCols(List<ColumnMetaData> orderedCols) {
		this.orderedCols = orderedCols;
	}

	public Map<String, ColumnMetaData> getUniqueMapping() {
		return uniqueMapping;
	}

	public void setUniqueMapping(Map<String, ColumnMetaData> uniqueMapping) {
		this.uniqueMapping = uniqueMapping;
	}

	public List<ColumnMetaData> getOrderedUniqueCols() {
		return orderedUniqueCols;
	}

	public void setOrderedUniqueCols(List<ColumnMetaData> orderedUniqueCols) {
		this.orderedUniqueCols = orderedUniqueCols;
	}

	public Map<String, ColumnMetaData> getColumnAllMapping() {
		return columnAllMapping;
	}

	public void setColumnAllMapping(Map<String, ColumnMetaData> columnAllMapping) {
		this.columnAllMapping = columnAllMapping;
	}

	public List<ColumnMetaData> getOrderedAllCols() {
		return orderedAllCols;
	}

	public void setOrderedAllCols(List<ColumnMetaData> orderedAllCols) {
		this.orderedAllCols = orderedAllCols;
	}

	public Map<String, String> getColumnNameMapping() {
		return columnNameMapping;
	}

	public void setColumnNameMapping(Map<String, String> columnNameMapping) {
		this.columnNameMapping = columnNameMapping;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
}
