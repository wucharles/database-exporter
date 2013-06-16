package org.db.export.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.db.export.common.runner.type.ExportMode;
import org.db.export.common.type.TableMetaData;
import org.db.export.common.type.Triple;
import org.db.export.common.util.CommonUtils;

public class ExporterContext {
	private Integer pageSize = null;
	
	private Set<String> originTables = new HashSet<String>();
	
	//private boolean clearTableData = false;
	
	private boolean clearSrcTablesData = false;
	
	private ExportMode exportMode = null;
	
	private Properties srcJdbcProperties = null;
	
	private Properties targetJdbcProperties = null;
	
	private List<Triple<String,String,String>> exprotTables = null;
	
	private Map<String, TableMetaData> targetTableMetaData = null;
	
	private Map<String, TableMetaData> srcTableMetaData = null;
	
	private boolean srcDbCaseSensitive = Constants.DEFAULT_SRC_DB_CASE_SENSITIVE;
	
	private boolean targetDbCaseSensitive = Constants.DEFAULT_DEST_DB_CASE_SENSITIVE;
	
	private Exporter srcExporter = null;
	
	private Exporter targetExporter = null;
	
	private String srcSchema = null;
	
	private String targetSchema = null;
	
	private Integer corePoolSize = null;
	
	private Integer maxPoolSize = null;
	
	private Integer parallelThreadSize = null;
	
	private Map<String, String> whereMapping = new HashMap<String,String>();
	
	private String defaultWhere = null;
	
	//the source table alias names in target database;
	private Map<String, Set<String>> tableAliasMapping = new HashMap<String, Set<String>>();

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public Set<String> getOriginTables() {
		return originTables;
	}

	public void setOriginTables(Set<String> originTables) {
		this.originTables = originTables;
	}

	/*public boolean isClearTableData() {
		return clearTableData;
	}

	public void setClearTableData(boolean clearTableData) {
		this.clearTableData = clearTableData;
	}*/

	public ExportMode getExportMode() {
		return exportMode;
	}

	public void setExportMode(ExportMode exportMode) {
		this.exportMode = exportMode;
	}

	public Properties getSrcJdbcProperties() {
		return srcJdbcProperties;
	}

	public void setSrcJdbcProperties(Properties srcJdbcProperties) {
		this.srcJdbcProperties = srcJdbcProperties;
	}

	public Properties getTargetJdbcProperties() {
		return targetJdbcProperties;
	}

	public void setTargetJdbcProperties(Properties targetJdbcProperties) {
		this.targetJdbcProperties = targetJdbcProperties;
	}

	public List<Triple<String,String,String>> getExprotTables() {
		return exprotTables;
	}

	public void setExprotTables(List<Triple<String,String,String>> exprotTables) {
		this.exprotTables = exprotTables;
	}

	public Map<String, TableMetaData> getTargetTableMetaData() {
		return targetTableMetaData;
	}

	public void setTargetTableMetaData(Map<String, TableMetaData> targetTableMetaData) {
		this.targetTableMetaData = targetTableMetaData;
	}

	public Map<String, TableMetaData> getSrcTableMetaData() {
		return srcTableMetaData;
	}

	public void setSrcTableMetaData(Map<String, TableMetaData> srcTableMetaData) {
		this.srcTableMetaData = srcTableMetaData;
	}
	
	/*public TableMetaData getTargetTableMetaData(String originTable) {
		return getTargetTableMetaData().get(getTargetTable(originTable));
	}*/
	
	//support table alias.
	public TableMetaData getTargetTableMetaData(String sourceTable, String targetTable) {
		TableMetaData md = getTargetTableMetaData().get(sourceTable + "-->" + targetTable);
		return md == null ? getTargetTableMetaData().get(targetTable) : md;
	}
	
	public String getTargetTable(String originTable) {
		return isTargetDbCaseSensitive() ? originTable : originTable.toUpperCase();
	}
	
	public TableMetaData getSrcTableMetaData(String originTable) {
		return getSrcTableMetaData().get(getSrcTable(originTable));
	}
	
	public String getSrcTable(String originTable) {
		return isSrcDbCaseSensitive() ? originTable : originTable.toUpperCase();
	}

	public boolean isSrcDbCaseSensitive() {
		return srcDbCaseSensitive;
	}

	public void setSrcDbCaseSensitive(boolean srcDbCaseSensitive) {
		this.srcDbCaseSensitive = srcDbCaseSensitive;
	}

	public boolean isTargetDbCaseSensitive() {
		return targetDbCaseSensitive;
	}

	public void setTargetDbCaseSensitive(boolean targetDbCaseSensitive) {
		this.targetDbCaseSensitive = targetDbCaseSensitive;
	}

	public Exporter getSrcExporter() {
		return srcExporter;
	}

	public void setSrcExporter(Exporter srcExporter) {
		this.srcExporter = srcExporter;
	}

	public Exporter getTargetExporter() {
		return targetExporter;
	}

	public void setTargetExporter(Exporter targetExporter) {
		this.targetExporter = targetExporter;
	}

	public String getSrcSchema() {
		return srcSchema;
	}
	
	public String getSrcSchemaPrefix() {
		return getSrcSchema() == null ? "" : getSrcSchema() + ".";
	}

	public void setSrcSchema(String srcSchema) {
		this.srcSchema = srcSchema;
	}

	public String getTargetSchema() {
		return targetSchema;
	}
	
	public String getTargetSchemaPrefix() {
		return getTargetSchema() == null ? "" : getTargetSchema() + ".";
	}

	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}

	public Integer getCorePoolSize() {
		return corePoolSize;
	}

	public void setCorePoolSize(Integer corePoolSize) {
		this.corePoolSize = corePoolSize;
	}

	public Integer getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(Integer maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public Integer getParallelThreadSize() {
		return parallelThreadSize;
	}

	public void setParallelThreadSize(Integer parallelThreadSize) {
		this.parallelThreadSize = parallelThreadSize;
	}

	public Map<String, String> getWhereMapping() {
		return whereMapping;
	}

	public void setWhereMapping(Map<String, String> whereMapping) {
		this.whereMapping = whereMapping;
	}

	public String getDefaultWhere() {
		return defaultWhere;
	}

	public void setDefaultWhere(String defaultWhere) {
		this.defaultWhere = defaultWhere;
	}
	
	public String getWhere(String sourceTable, String targetTable, String originTable) {
		String where = getWhere(originTable, false);
		
		if(CommonUtils.isBlank(where)) {
			where = getWhere(sourceTable, false);
		}
		
		if(CommonUtils.isBlank(where)) {
			where = getWhere(targetTable, true);
		}
		
		return where;
	}
	
	private String getWhere(String originTableName, boolean usingDefault) {
		String where = getWhereMapping().get(originTableName);
		if(where == null) {
			where = getWhereMapping().get(getSrcTable(originTableName));
		}
		
		if(where == null) {
			where = getWhereMapping().get(originTableName.toUpperCase());
		}
		
		if(where == null) {
			where = getWhereMapping().get(originTableName.toLowerCase());
		}
		
		if(where == null && usingDefault) {
			where = getDefaultWhere();
		}
		
		return where == null ? "" : where;
	}

	public boolean isClearSrcTablesData() {
		return clearSrcTablesData;
	}

	public void setClearSrcTablesData(boolean clearSrcTablesData) {
		this.clearSrcTablesData = clearSrcTablesData;
	}

	public Map<String, Set<String>> getTableAliasMapping() {
		return tableAliasMapping;
	}

	public void setTableAliasMapping(Map<String, Set<String>> tableAliasMapping) {
		this.tableAliasMapping = tableAliasMapping;
	}
}
