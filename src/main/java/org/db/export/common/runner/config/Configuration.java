package org.db.export.common.runner.config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.db.export.common.Constants;
import org.db.export.common.Environment;
import org.db.export.common.Exporter;
import org.db.export.common.ExporterContext;
import org.db.export.common.RunnerException;
import org.db.export.common.log.Logger;
import org.db.export.common.log.LoggerFactory;
import org.db.export.common.runner.type.ExportMode;
import org.db.export.common.type.ColumnMetaData;
import org.db.export.common.type.MetaDataStatus;
import org.db.export.common.type.TableMetaData;
import org.db.export.common.type.Triple;
import org.db.export.common.util.CommonUtils;
import org.db.export.common.util.DataSourceUtils;
import org.db.export.common.util.IOUtils;
import org.db.export.common.util.PropertyUtils;

public class Configuration {
	private final static Logger log = LoggerFactory.getLogger(Configuration.class);
	
	private ExporterContext context = null;
	
	//context class loader.
	private ClassLoader loader = null;
	
	//data export mode: full export(insert) or increment export(update)
	private ExportMode exportMode = null;
	
	//the page size of per select from source database.
	private Integer pageSize = null;
	
	//the core pool size of the Executor Service(Thread Pool);
	private Integer corePoolSize = null;
	
	//the max pool size of the Executor Service(Thread Pool);
	private Integer maxPoolSize = null;
	
	//the parallel threads per table.
	private Integer parallelThreadSize = null;
	
	//the default where condition for select statement on per table.
	private String defaultWhere = null;
	
	//default sql to be execute after table data exported;
	private String defaultPostSQL = null;
	
	//the schema of source database;
	private String srcSchema = null;
	
	//the schema of destination database;
	private String targetSchema = null;
	
	//configure whether clear table data before export data from destination database.
	//private boolean clearTableData = Constants.DEFAULT_CLEAR_TABLE_DATA;
	
	//Whether string is case sensitive in source database;
	private boolean srcDbCaseSensitive = Constants.DEFAULT_SRC_DB_CASE_SENSITIVE;
	
	//Whether string is case sensitive in destination database;
	private boolean targetDbCaseSensitive = Constants.DEFAULT_DEST_DB_CASE_SENSITIVE;
	
	//the tables for property "TABLES", is case sensitive.The separator of table are ',', ';'.
	private Set<String> originTables = new HashSet<String>();
	
	//Source database tables(case sensitive?); If not case sensitive, saved with upper case.
	private Set<String> srcTables = new HashSet<String>();
	
	//Destination database tables(case sensitive?); If not case sensitive, saved with upper case.
	private Set<String> targetTables = new HashSet<String>();
	
	//Where condition for source database table;
	private Map<String, String> whereMapping = new HashMap<String,String>();
	
	//the source database Exporter instance.
	private Exporter srcExporter = null;
	
	//the destination database Exporter instance.
	private Exporter targetExporter = null;
	
	//the common properties of exporter configuration.
	private Properties commonProperties = null;
	
	//the database related properties of exporter configuration.
	private Properties databaseProperties = null;
	
	//mapping source table columns to target table columns;
	private Properties mappingProperties = null;
	
	//the JDBC properties of source database.
	private Properties srcJdbcProperties = null;
	
	//the JDBC properties of destination database.
	private Properties targetJdbcProperties = null;
	
	//the destination tables meta data.
	private Map<String, TableMetaData> targetTableMetaData = null;
	
	//the destination tables meta data.
	private Map<String, TableMetaData> srcTableMetaData = null;
	
	//before export data to source database, clear all table data?
	private boolean clearSrcTablesData =  Constants.DEFAULT_CLEAR_TABLE_DATA;
	
	//The tables need to be exported.
	private List<Triple<String,String,String>> exportTables = null;
	
	//the table mapping list;
	private Map<String, Map<String,Object>> mappingJson = new HashMap<String, Map<String,Object>>();
	
	//the source table alias names in target database;
	private Map<String, Set<String>> tableAliasMapping = new HashMap<String, Set<String>>();
	
	//the file to write export sql
	private String exportSqlFile = Environment.getRootPath() + File.separator + Constants.CONST_SQL_FILE;
	
	//command line args
	private String[] args = null;
	
	public Configuration() {
		;
	}
	
	private void genSQL() {
		BufferedWriter writer = null;
		try {
			String fileStr = getExportSqlFile();
			File file = new File(fileStr);
			if(!file.exists()) {
				file.createNewFile();
			}
			if(!file.canWrite()) {
				file = File.createTempFile(Constants.CONST_SQL_FILE_PREFIX, Constants.CONST_SQL_FILE_SUFFIX);
				
				setExportSqlFile(file.getCanonicalFile().getAbsolutePath());
			}
			writer = new BufferedWriter(new FileWriter(file));
			
			for(Triple<String,String,String> triple : getExportTables()) {
				genSQL(writer, triple.value2, triple.value3, triple.value1);
			}
			
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}
	
	private void genSQL(BufferedWriter writer, String sourceTable, String targetTable, String originTable) throws IOException {
		TableMetaData targetTableMetaData = context.getTargetTableMetaData(sourceTable, targetTable);
		TableMetaData srcTableMetaData = context.getSrcTableMetaData().get(sourceTable);
		if(targetTableMetaData == null || srcTableMetaData == null) {
			return;
		}
		
		String where = context.getWhere(sourceTable, targetTable, originTable);
		String select = context.getSrcExporter().genSelectDML(context.getSrcSchemaPrefix(),
				sourceTable, srcTableMetaData.getOrderedCols(), where);
		String updateOrInsert = context.getExportMode() == ExportMode.FULL ? 
				context.getTargetExporter().genInsertDML(context.getTargetSchemaPrefix()
						, targetTable, targetTableMetaData.getOrderedCols())
						: context.getTargetExporter().genUpdateDML(context.getTargetSchemaPrefix()
								, targetTable, targetTableMetaData.getOrderedCols()
								, targetTableMetaData.getOrderedUniqueCols());
				
		writer.write("--table name: " + sourceTable + "/" + targetTable + "\n");
		writer.write(select);
		writer.write("\n");
		writer.write(updateOrInsert);
		writer.write("\n");
		writer.write("\n");
	}
	
	private void printConfigInfo() {
		StringBuilder info = new StringBuilder("\nDatabase Exporter Configuration:\n");
		
		//数据库类型
		info.append(Constants.CONST_PRINT_INDENT)
			 .append("Source database type: " + getSrcExporter().getDbName())
			 .append(", schema = ").append(getSrcSchema()).append("\n\n");
		info.append(Constants.CONST_PRINT_INDENT)
			 .append("Target database type: " + getTargetExporter().getDbName())
		     .append(", schema = ").append(getTargetSchema()).append("\n\n");
		
		//导出模式
		info.append(Constants.CONST_PRINT_INDENT)
			.append("The import/export SQL: ").append(getExportSqlFile()).append("\n\n");
		
		//导出模式
		info.append(Constants.CONST_PRINT_INDENT)
			.append("Database export model: " + exportMode).append("\n\n");
		
		//预处理
		info.append(Constants.CONST_PRINT_INDENT)
			.append("Whether clear all target table data before exporting: " + isClearSrcTablesData()).append("\n\n");
		
		//执行属性
		info.append(Constants.CONST_PRINT_INDENT).append("The export runner config\n")
			.append(Constants.CONST_PRINT_INDENTx2)
			.append("The core size of thread-pool: ").append(getCorePoolSize()).append("\n\n")
			.append(Constants.CONST_PRINT_INDENTx2)
			.append("The max size of thread-pool: ").append(getMaxPoolSize()).append("\n\n")
			.append(Constants.CONST_PRINT_INDENTx2)
			.append("The page size of fetching data: ").append(getPageSize()).append("\n\n")
			.append(Constants.CONST_PRINT_INDENTx2)
			.append("The threads of per table: ").append(getParallelThreadSize()).append("\n\n");
		
		//TODO: post sql?
		//info.append("\t每个表导出完毕后需要执行的SQL：").append((POST_SQL == null ? "无" : POST_SQL)).append("\n");
		
		Set<String> tables = new HashSet<String>(getExportTables().size());
		Set<String> onlyInSrc = new HashSet<String>(getSrcTables().size());
		Set<String> onlyInTarget = new HashSet<String>(getTargetTables().size());
		for(Triple<String,String,String> table : getExportTables()) {
			tables.add(table.value1);
		}
		for(TableMetaData table : getSrcTableMetaData().values()) {
			if(table.getStatus() != MetaDataStatus.NORMAL) {
				onlyInSrc.add(table.getName());
			}
		}
		for(TableMetaData table : getTargetTableMetaData().values()) {
			if(table.getStatus() != MetaDataStatus.NORMAL) {
				onlyInTarget.add(table.getName());
			}
		}
		info.append(Constants.CONST_PRINT_INDENT).append("Table information\n");
		
		printExportTableInfo(info, "The tables need to be exported", tables);
		
		printExportTableInfo(info, "The tables only exists in source databas", onlyInSrc);
		
		printExportTableInfo(info, "The tables only exists in target databas", onlyInTarget);
		
		log.info(info.toString());
	}
	
	private static void printExportTableInfo(StringBuilder buffer, String title, Set<String> tables) {
		buffer.append(Constants.CONST_PRINT_INDENTx2).append(title);
		if(tables == null || tables.size() <= 0) {
			buffer.append("\n").append(Constants.CONST_PRINT_INDENTx3).append("----\n\n");
			
			return;
		}
		
		buffer.append("\n").append(Constants.CONST_PRINT_INDENTx3);
		List<List<String>> cells = CommonUtils.slice(new ArrayList<String>(tables), 5);
		for(List<String> cell : cells) {
			for(String table : cell) {
				buffer.append(table).append(", ");
			}
			buffer.setLength(buffer.length() - 2);
			buffer.append("\n").append(Constants.CONST_PRINT_INDENTx3);
		}
		buffer.setLength(buffer.length() - Constants.CONST_PRINT_INDENTx3.length());
		buffer.append("\n");
	}
	
	private void parseCommandLine(final String[] args) {
		//解析命令行清理参数
		parseClearData(args);
	}
	
	private void parseClearData(final String[] args) {
		if(args != null && args.length > 0) {
			boolean clearData = false;
			try {
				clearData = Boolean.parseBoolean(args[0]);
			} catch (Exception e) {
				log.warn("命令行第一个参数应该是boolean类型，表示是否需要清理目标数据库中的数据.");
			}
			
			setClearSrcTablesData(clearData);
		}
	}
	
	private void parseTableMetaData() {
		//destination database.
		Connection con = null;
		try {
			targetTableMetaData = new HashMap<String, TableMetaData>(targetTables.size(), 1F);
			con = DataSourceUtils.con(getTargetJdbcProperties());
			parseTableMetaData(con, targetTableMetaData, getTargetExporter()
					, getTargetSchema(), getTargetTables(), isTargetDbCaseSensitive());
			
			for(String table : getOriginTables()) {
				
				Set<String> aliases = getTableAliasMapping().get(table);
				
				//add column name mapping;
				if (CommonUtils.isEmpty(aliases)) {
					String targetTable = getTargetTable(table);
					addColumnNameMapping4Target(targetTableMetaData.get(targetTable), table);
				} else {
					for(String alias : aliases) {
						String targetTable = getTargetTable(alias);
						
						TableMetaData copy = targetTableMetaData.get(targetTable).copy();
						targetTableMetaData.put(getSrcTable(table) + "-->" + targetTable, copy);
						addColumnNameMapping4Target(copy, table);
					}
				}
			}
			
			addSourceName4Target(targetTableMetaData);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceUtils.closeQuietly(con);
		}
		
		//source database.
		try {
			srcTableMetaData = new HashMap<String, TableMetaData>(srcTables.size(), 1F);
			con = DataSourceUtils.con(getSrcJdbcProperties());
			parseTableMetaData(con, srcTableMetaData, getSrcExporter()
					, getSrcSchema(), getSrcTables(), isSrcDbCaseSensitive());
			
			for(String table : getOriginTables()) {
				String sourceTable = getSrcTable(table);
				
				//add column name mapping;
				addColumnNameMapping4Source(srcTableMetaData.get(sourceTable), table);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceUtils.closeQuietly(con);
		}
		
		//validate the table metadata;
		checkMetaData();
		
		//add export table list
		setExportTables(new ArrayList<Triple<String,String,String>>(getOriginTables().size()));
		for(String table : getOriginTables()) {
			final Set<String> aliases = getTableAliasMapping().get(table);
			
			//target table maybe has alias name.
			if(CommonUtils.isEmpty(aliases)) {
				addExportTables(table, getSrcTable(table), getTargetTable(table));
			} else {
				for(String alias : aliases) {
					addExportTables(table, getSrcTable(table), getTargetTable(alias));
				}
			}
			
		}
	}

	private void addSourceName4Target(
			Map<String, TableMetaData> mapping) {
		if(CommonUtils.isEmpty(mapping)) {
			return;
		}
		
		for(String table : getOriginTables()) {
			String tableName = getTargetTable(table);
			if(mapping.containsKey(tableName)) {
				mapping.get(tableName).setSourceName(getSrcTable(table));
			}
		}
	}

	private void checkMetaData() {
		checkMetaData4Src(getOriginTables(), srcTableMetaData, targetTableMetaData, MetaDataStatus.ONLY_IN_SRC);
		
		checkMetaData4Target(getOriginTables(), srcTableMetaData, targetTableMetaData, MetaDataStatus.ONLY_IN_DEST);
	}
	
	private void addExportTables(String originTable, String sourceTable, String targetTable) {
		final TableMetaData srcMeta = getSrcTableMetaData().get(sourceTable);
		final TableMetaData targetMeta = getTargetTableMetaData().get(targetTable);
		
		if(srcMeta != null && targetMeta != null
				&& srcMeta.getStatus() == MetaDataStatus.NORMAL
				&& targetMeta.getStatus() == MetaDataStatus.NORMAL) {
			Triple<String,String,String> t = new Triple<String,String,String>();
			t.value1 = originTable;
			t.value2 = sourceTable;
			t.value3 = targetTable;
			getExportTables().add(t);
		}
		
		if(srcMeta == null && targetMeta == null) {
			log.warn("Table[" + originTable + "]not exists in source and target database.");
		}
	}
	
	private void checkMetaData4Target(Set<String> tables,  Map<String, TableMetaData> src,
			Map<String, TableMetaData> target, MetaDataStatus toStatus) {
		for(String table : tables) {
			final Set<String> aliases = getTableAliasMapping().get(table);
			
			if(CommonUtils.isEmpty(aliases)) {
				checkTargetTableMetaData(src, target, getSrcTable(table), getTargetTable(table), toStatus);
			} else {
				for(String alias: aliases) {
					checkTargetTableMetaData(src, target, getSrcTable(table), getTargetTable(alias), toStatus);
				}
			}
		}
	}
	
	public void checkTargetTableMetaData(Map<String, TableMetaData> src, Map<String, TableMetaData> target,
			String sourceTable, String targetTable, MetaDataStatus toStatus) {
		final TableMetaData targetMD = getTargetTableMetaData(sourceTable, targetTable);
		
		if( targetMD == null ) {
			return;
		}
		
		if(!src.containsKey(sourceTable)) {
			targetMD.setStatus(toStatus);
			
			return;
		}
		
		final TableMetaData srcMD = src.get(sourceTable);
		for(ColumnMetaData col : targetMD.getOrderedAllCols()) {
			if(!srcMD.getColumnAllMapping().containsKey(col.getSourceName())) {
				col.setStatus(toStatus);
			}
		}
	}
	
	private void checkMetaData4Src(Set<String> tables,  Map<String, TableMetaData> src,
			Map<String, TableMetaData> target, MetaDataStatus toStatus) {
		for(String table : tables) {
			final Set<String> aliases = getTableAliasMapping().get(table);
			
			if(CommonUtils.isEmpty(aliases)) {
				checkSrcTableMetaData(src, target, getSrcTable(table), getTargetTable(table), toStatus);
			} else {
				for(String alias: aliases) {
					checkSrcTableMetaData(src, target, getSrcTable(table), getTargetTable(alias), toStatus);
				}
			}
		}
	}
	
	public void checkSrcTableMetaData(Map<String, TableMetaData> src, Map<String, TableMetaData> target,
			String sourceTable, String targetTable, MetaDataStatus toStatus) {
		TableMetaData srcMD = src.get(sourceTable);
		
		if( srcMD == null ) {
			return;
		}
		
		if(!target.containsKey(targetTable)) {
			srcMD.setStatus(toStatus);
			
			return;
		}
		
		final TableMetaData targetMD = getTargetTableMetaData(sourceTable, targetTable);
		for(ColumnMetaData col : srcMD.getOrderedAllCols()) {
			if(!targetMD.getColumnAllMapping().containsKey(col.getSourceName())) {
				col.setStatus(toStatus);
			}
		}
	}

	private void parseTableMetaData(Connection con,
			Map<String, TableMetaData> tableMetaData, Exporter exporter,
			String schema, Set<String> tables, boolean isDbCaseSensitive)
			throws SQLException {
		for (String table : tables) {
			ResultSet rs = con.getMetaData().getColumns(
					exporter.getCatalog(null, schema, isDbCaseSensitive),
					exporter.getSchema(schema, isDbCaseSensitive), table, null);

			readTableMetadata(table, rs, tableMetaData, exporter);
			DataSourceUtils.closeQuietly(rs);
		}
	}

	private void addColumnNameMapping4Target(TableMetaData tableMetaData,
			String sourceTable) {
		if(tableMetaData == null) {
			return;
		}
		
		Map<String, Object> mapping = getMappingJson().get(sourceTable);
		if(mapping == null) {
			return;
		}
		
		Set<Entry<String, Object>> entries = mapping.entrySet();
		for(Entry<String, Object> entry : entries) {
			tableMetaData.getColumnNameMapping().put(entry.getValue().toString().trim().toUpperCase()
					, entry.getKey().trim().toUpperCase());
		}
	}
	
	private void addColumnNameMapping4Source(TableMetaData tableMetaData,
			String originTable) {
		if(tableMetaData == null) {
			return;
		}
		
		Map<String, Object> mapping = getMappingJson().get(originTable);
		if(mapping == null) {
			return;
		}
		
		Set<Entry<String, Object>> entries = mapping.entrySet();
		for(Entry<String, Object> entry : entries) {
			tableMetaData.getColumnNameMapping().put(entry.getKey().trim().toUpperCase()
					, entry.getValue().toString().trim().toUpperCase());
		}
	}

	private void readTableMetadata(String table, ResultSet rs,
			Map<String, TableMetaData> tableMetaData, Exporter exporter) throws SQLException {
		if(rs == null || !rs.next()) {
			return;
		}
	
		TableMetaData meta = new TableMetaData();
		meta.setName(table);
		meta.setColumnMapping(new HashMap<String, ColumnMetaData>(30, 1F));
		tableMetaData.put(table, meta);
		
		do{
			ColumnMetaData column = new ColumnMetaData(meta);
			column.setName(rs.getString("COLUMN_NAME").toUpperCase());
			column.setType(rs.getInt("DATA_TYPE"));
			column.setSize(rs.getInt("COLUMN_SIZE"));
			column.setDigits(rs.getInt("DECIMAL_DIGITS"));
			column.setDefaultValue(rs.getString("COLUMN_DEF"));
			
			int nullable 		= rs.getInt("NULLABLE");
			switch(nullable) {
			case DatabaseMetaData.columnNoNulls:
				column.setNullable(false);
				break;
			case DatabaseMetaData.columnNullable:
				column.setNullable(true);
				break;
			case DatabaseMetaData.columnNullableUnknown:
				column.setNullable(true);
				break;
			default:
				column.setNullable(true);
				break;
			}
			
			column.setType(exporter.filterDataType(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME")));
			
			meta.getColumnMapping().put(column.getName(), column);
		} while(rs.next());

		//parse update unique columns;
		if(exportMode == ExportMode.INC) {
			parsePerTableUnique(meta);
		}
		
		meta.setOrderedUniqueCols(new ArrayList<ColumnMetaData>(meta.getUniqueMapping().values()));
		meta.setOrderedCols(new ArrayList<ColumnMetaData>(meta.getColumnMapping().values()));
		
		meta.setColumnAllMapping(new HashMap<String, ColumnMetaData>(meta.getColumnMapping()));
		meta.getColumnAllMapping().putAll(meta.getUniqueMapping());
		meta.setOrderedAllCols(new ArrayList<ColumnMetaData>(meta.getOrderedCols()));
		meta.getOrderedAllCols().addAll(meta.getOrderedUniqueCols());
	}
	
	private void parsePerTableUnique(TableMetaData meta) {
		final String prefix = "UNIQUE_COLS_";

		Set<Object> keys = getCommonProperties().keySet();
		for(String table :  getTargetTables()) {
			for(Object key : keys) {
				String _keyStr = key.toString();
				String keyStr = _keyStr.replace("%", "[a-zA-Z0-9]*");
				Pattern ptn = Pattern.compile(keyStr);

				//upper case.
				Matcher matcher = ptn.matcher(prefix + table);
				if(matcher.matches()) {
					parsePerTableUnique(meta, PropertyUtils.getTrimProperty(getCommonProperties(), _keyStr));

					break;
				}

				if(isTargetDbCaseSensitive()) {
					continue;
				}

				//lower case.
				matcher = ptn.matcher(prefix + table.toLowerCase());
				if(matcher.matches()) {
					parsePerTableUnique(meta, PropertyUtils.getTrimProperty(getCommonProperties(), _keyStr));

					break;
				}

				//origin. TODO: ??
				/*matcher = ptn.matcher(prefix + originTablesMapping.get(table));
				if(matcher.matches()) {
					parsePerTableUnique(meta, COMMON.getProperty(_keyStr));

					break;
				}*/

				//default
				if(meta.getUniqueMapping() == null
						|| meta.getUniqueMapping().isEmpty()) {
					parsePerTableUnique(meta, PropertyUtils.getTrimProperty(getCommonProperties(), Constants.CONST_DEFAULT_UNIQUE_COLS));
				}
			}
		}
	}
	
	private void parsePerTableUnique(TableMetaData meta, String property) {
		if(property == null || property.isEmpty()) {
			return;
		}

		meta.setUniqueMapping(new HashMap<String, ColumnMetaData>());
		StringTokenizer tokens = new StringTokenizer(property.trim(), ",;");
		while(tokens.hasMoreElements()) {
			String column = tokens.nextToken();
			if(column != null && !(column = column.trim()).isEmpty()) {
				column = column.toUpperCase();
				meta.getUniqueMapping().put(column, meta.getColumnMapping().get(column));
				meta.getColumnMapping().remove(column);
			}
		}
	}
	
	private void parseTableMapping() {
		Properties prop = PropertyUtils.load(getLoader(), Constants.CONST_MAPPING_PROP_FILE);
		setMappingProperties(prop);
		
		Set<Entry<Object, Object>> entries = prop.entrySet();
		for(Entry<Object, Object> entry : entries) {
			String name = entry.getKey().toString();
			String value = entry.getValue().toString();
			
			try {
				Map<String,Object> json = CommonUtils.toJSON(value);
				
				Set<Entry<String, Object>> jsonEntries = json.entrySet();
				for(Entry<String, Object> jsonEntry : jsonEntries) {
					if(!(jsonEntry.getValue() instanceof String)) {
						new RunnerException("table columns mapping error, column name must be string:" +
								" tableName =  " + name + ", columnName = " + jsonEntry.getValue());
					}
				}
				
				getMappingJson().put(name.trim(), json);
			} catch (Exception e) {
				new RunnerException("table columns mapping error: ", e);
			}
		}
	}

	private void parseDbInfo() {
		//jdbc driver;
		try {
			Class.forName(getSrcJdbcProperties().getProperty(Constants.CONST_JDBC_DRIVER_CLASSNAME));
			Class.forName(getTargetJdbcProperties().getProperty(Constants.CONST_JDBC_DRIVER_CLASSNAME));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			
			throw new RuntimeException("Please configure source and target database JDBC driver class, reference: jdbc.driverClassName.");
		}
		
		//src db case sensitive?
		Connection con = DataSourceUtils.con(getSrcJdbcProperties());
		boolean cs = getSrcExporter().isCaseSensitive(con);
		setSrcDbCaseSensitive(cs);

		//dest db case sensitive?
		con = DataSourceUtils.con(getTargetJdbcProperties());
		cs = getTargetExporter().isCaseSensitive(con);
		setTargetDbCaseSensitive(cs);

		//parse schem if not set.
		parseSchema();
	}

	private void parseSchema() {
		if(getSrcSchema() == null) {
			String jdbcUrl = getSrcJdbcProperties().getProperty(Constants.CONST_JDBC_URL);
			String userName = getSrcJdbcProperties().getProperty(Constants.CONST_JDBC_USERNAME);
			setSrcSchema(getSrcExporter().getSchemaName(jdbcUrl, userName));
		}
		
		if(getSrcSchema() == null) {
			String jdbcUrl = getTargetJdbcProperties().getProperty(Constants.CONST_JDBC_URL);
			String userName = getTargetJdbcProperties().getProperty(Constants.CONST_JDBC_USERNAME);
			setTargetSchema(getTargetExporter().getSchemaName(jdbcUrl, userName));
		}
	}

	private void parseDbExporter(List<Exporter> exporters) {
		//source database.
		String name = fetchDBProductName(DataSourceUtils.con(getSrcJdbcProperties()));
		for(Exporter exporter : exporters) {
			if(exporter.isSupport(name)) {
				setSrcExporter(exporter);
				
				System.out.println(">> Src exporter: " + getSrcExporter().getDescription());
				break;
			}
		}
		
		//destination database.
		name = fetchDBProductName(DataSourceUtils.con(getTargetJdbcProperties()));
		for(Exporter exporter : exporters) {
			if(exporter.isSupport(name)) {
				setTargetExporter(exporter);
				
				System.out.println(">> Target exporter: " + getTargetExporter().getDescription());
				break;
			}
		}
	}
	
	private static String fetchDBProductName(Connection con) {
		String name = null;
		
		try {
			DatabaseMetaData meta = con.getMetaData();
			name = meta.getDatabaseProductName();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceUtils.closeQuietly(con);
		}
		
		return name;
	}

	private List<Exporter> findAllExporters() {
		try {
			Enumeration<URL> files = getLoader().getResources(Constants.CONST_EXPORTER_FILE);
			if(files == null) {
				return new ArrayList<Exporter>(0);
			}
			
			List<Exporter> instances = new ArrayList<Exporter>();
			while(files.hasMoreElements()) {
				URL file = files.nextElement();
				String qualifiedClass = IOUtils.readLine(file);
				if(qualifiedClass == null || qualifiedClass.isEmpty()) {
					continue;
				}
				
				try {
					Class<?> clzz = getLoader().loadClass(qualifiedClass);
					if(Exporter.class.isAssignableFrom(clzz)) {
						instances.add((Exporter)clzz.newInstance());
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
			
			//order.
			Collections.sort(instances, new Comparator<Exporter>() {
				@Override
				public int compare(Exporter o1, Exporter o2) {
					if(o1.getOrder() > o2.getOrder()) {
						return -1;
					} else if(o1.getOrder() < o2.getOrder()) {
						return 1;
					}
					
					return 0;
				}
			});
			
			return instances;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void parseCommonConfig() {
		Properties prop = PropertyUtils.load(getLoader(), Constants.CONST_COMMON_PROP_FILE);
		setCommonProperties(prop);
		
		//export mode value;
		String modeProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_EXPORT_MODE, null);
		setExportMode(ExportMode.getByName(modeProp));
		
		//clear table data?
		boolean clearTableDataProp = PropertyUtils.getBoolean(prop
				, Constants.CONST_CLEAR_TABLES_DATA, Constants.DEFAULT_CLEAR_TABLE_DATA);
		setClearSrcTablesData(clearTableDataProp);
		if(getExportMode() == ExportMode.INC) {
			setClearSrcTablesData(false);
		}
		
		//select page size;
		Integer pageProp = PropertyUtils.getInteger(prop
				, Constants.CONST_PAGE_SIZE, Constants.DEFAULT_PAGE_SIZE);
		setPageSize(pageProp);
		
		//core pool size;
		Integer corePoolSizeProp = PropertyUtils.getInteger(prop
				, Constants.CONST_CORE_POOL_SIZE, Constants.DEFAULT_CORE_POOL_SIZE);
		setCorePoolSize(corePoolSizeProp);
		
		//max pool size;
		Integer maxPoolSizeProp = PropertyUtils.getInteger(prop
				, Constants.CONST_MAX_POOL_SIZE, Constants.DEFAULT_MAX_POOL_SIZE);
		setMaxPoolSize(maxPoolSizeProp);
		
		//parallel thread size;
		Integer paraThreadSizeProp = PropertyUtils.getInteger(prop
				, Constants.CONST_PARALLEL_THREAD_SIZE, Constants.DEFAULT_PARALLEL_THREAD_SIZE);
		setParallelThreadSize(paraThreadSizeProp);
	}
	
	private void parseDatabaseConfig() {
		Properties prop = PropertyUtils.load(getLoader(), Constants.CONST_DATABASE_PROP_FILE);
		setDatabaseProperties(prop);
		
		//default where condition for select exported data value;
		String defaultWhereProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_DEFAULT_WHERE, Constants.DEFAULT_WHERE);
		setDefaultWhere(defaultWhereProp);
		
		//default sql to be execute after table data exported;
		String defaultPostSQLProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_DEFAULT_POST_SQL, null);
		setDefaultPostSQL(defaultPostSQLProp);
		
		//source schema;
		String srcSchemaProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_SRC_SCHEMA, null);
		setSrcSchema(srcSchemaProp);
		
		//dest schema;
		String targetSchemaProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_DEST_SCHEMA, null);
		setTargetSchema(targetSchemaProp);
		
		//charleswu[2013-06-11]: fetch info from database;
		//src db case sensitive?
		/*boolean srcDbCaseSenProp = PropertyUtils.getBoolean(prop
				, Constants.CONST_SRC_DB_CASE_SENSITIVE, Constants.DEFAULT_SRC_DB_CASE_SENSITIVE);
		setSrcDbCaseSensitive(srcDbCaseSenProp);*/
		
		//dest db case sensitive?
		/*boolean targetDbCaseSenProp = PropertyUtils.getBoolean(prop
				, Constants.CONST_DEST_DB_CASE_SENSITIVE, Constants.DEFAULT_DEST_DB_CASE_SENSITIVE);
		setTargetDbCaseSensitive(targetDbCaseSenProp);*/
		
		//parse table alias.
		String aliasJson = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_EXPORT_TABLE_ALIAS, null);
		parseTableAlias(aliasJson);
		
		//configured tables
		String tablesProp = PropertyUtils.getTrimProperty(prop
				, Constants.CONST_TABLES, null);
		parseTables(tablesProp);
		
		//parse where condition for source database table.
		parsePerTableWhere(prop);
	}

	@SuppressWarnings("unchecked")
	private void parseTableAlias(String aliasJson) {
		if(CommonUtils.isBlank(aliasJson)) {
			return;
		}
		
		try {
			Map<String, Object> json = CommonUtils.toJSON(aliasJson);
			
			Set<Map.Entry<String, Object>> entries = json.entrySet();
			for(Map.Entry<String, Object> entry : entries) {
				if(!(entry.getValue() instanceof List)) {
					throw new RunnerException("Table alias value must be array: key = " + entry.getKey()
							+ ", value = "+ entry.getValue());
				}
				
				String key = entry.getKey();
				List<String> values = (List<String>)entry.getValue();
				Set<String> aliasSet = new HashSet<String>(values.size(), 1F);
				for(String value : values) {
					aliasSet.add(value);
				}
				
				getTableAliasMapping().put(key, aliasSet);
			}
		} catch (RunnerException e) {
			throw new RuntimeException(e);
		} catch (Exception e) { // others ignore
			e.printStackTrace();
		}
	}

	private void parsePerTableWhere(Properties prop) {
		Set<Object> keys = prop.keySet();
		
		for(String table : getOriginTables()) {
			Set<String> aliases = getTableAliasMapping().get(table);
			
			parseTableWhere(prop, keys, table);
			if(!CommonUtils.isEmpty(aliases)) {
				for(String alias : aliases) {
					parseTableWhere(prop, keys, alias);
				}
			}
		}
	}
	
	private void parseTableWhere(Properties prop, Set<Object> keys, String tableName) {
		final String prefix = Constants.CONST_WHERE_PREFIX;
		
		for(Object key : keys) {
			String _keyStr = key.toString();
			String keyStr = _keyStr.replace("%", "[a-zA-Z0-9]*");
			Pattern ptn = Pattern.compile(keyStr);
			
			//upper case.
			Matcher matcher = ptn.matcher(prefix + tableName);
			if(matcher.matches()) {
				whereMapping.put(tableName, prop.getProperty(_keyStr));
				
				break;
			}
			
			//case sensitive ?
			if(isSrcDbCaseSensitive()) {
				continue;
			}
			
			//lower case.
			matcher = ptn.matcher(prefix + tableName.toLowerCase());
			if(matcher.matches()) {
				whereMapping.put(tableName, prop.getProperty(_keyStr));
				
				break;
			}
			
			//origin. TODO: ??
			/*matcher = ptn.matcher(prefix + originTablesMapping.get(table));
			if(matcher.matches()) {
				whereMapping.put(table, prop.getProperty(_keyStr));
				
				break;
			}*/
		}
	}

	private void parseTables(String tablesProp) {
		if(tablesProp == null || tablesProp.length() <= 0) {
			return;
		}
		
		StringTokenizer tokens = new StringTokenizer(tablesProp, ",;");
		while(tokens.hasMoreElements()) {
			String table = tokens.nextToken();
			if(table == null || (table = table.trim()).isEmpty()) {
				continue;
			}
			
			getOriginTables().add(table);
		}
		
		//set source database tables(case sensitive?)
		setTables(getOriginTables(), getSrcTables(), isSrcDbCaseSensitive(), false);
		//set destination database tables (case sensitive?)
		setTables(getOriginTables(), getTargetTables(), isTargetDbCaseSensitive(), true);
		
		validateTableNames(getOriginTables(), isSrcDbCaseSensitive(), isTargetDbCaseSensitive());
	}
	
	private void validateTableNames(Set<String> originTables,
			boolean srcDbCaseSensitive, boolean targetDbCaseSensitive) {
		if(srcDbCaseSensitive && targetDbCaseSensitive) {
			return; // all case sensitive will pass directly.
		}
		
		Map<String,String> temp = new HashMap<String,String>(originTables.size());
		for(String table : originTables) {
			int oldSize = temp.size();
			temp.put(table.toLowerCase(), table);
			if(temp.size() == oldSize) {
				throw new IllegalArgumentException("Table name is not case sensitive, but has same name in upper case: " + table);
			}
		}
	}

	private void setTables(Set<String> originTables
			, Set<String> tables, boolean caseSensitive, boolean useAlias) {
		for(String table : originTables) {
			if(useAlias) {
				Set<String> aliases = getTableAliasMapping().get(table);
				if(CommonUtils.isEmpty(aliases)) {
					tables.add((caseSensitive ? table : table.toUpperCase()));
				} else {
					for(String alias : aliases) {
						tables.add((caseSensitive ? alias : alias.toUpperCase()));
					}
				}
			} else {
				tables.add((caseSensitive ? table : table.toUpperCase()));
			}
		}
	}

	private void prepareClassLoader() {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		if(loader == null) {
			loader = Exporter.class.getClassLoader();
		}
		
		if(loader == null) {
			throw new IllegalArgumentException("App stop with exception, conn't find 'ClassLoader'.");
		}
		
		setLoader(loader);
	}

	public ExporterContext config() {
		prepareClassLoader();
		
		//exporter for database;
		parseDbExporter();
		
		parseCommonConfig();
		
		parseDatabaseConfig();
		
		parseDbInfo();
		
		//config/mapping.properties
		parseTableMapping();
		
		parseTableMetaData();
		
		parseCommandLine(getCommandLine());
		
		buildContext();
		
		genSQL();
		printConfigInfo();
		
		
		return context;
	}
	
	private void parseDbExporter() {
		Properties src = PropertyUtils.load(getLoader(), Constants.CONST_SRC_PROP_FILE);
		setSrcJdbcProperties(src);
		Properties target = PropertyUtils.load(getLoader(), Constants.CONST_DEST_PROP_FILE);
		setTargetJdbcProperties(target);
		
		//find all db "Exporter";
		List<Exporter> exporters = findAllExporters();
		parseDbExporter(exporters);
	}

	private void buildContext() {
		context = new ExporterContext();
		
		getContext().setPageSize(getPageSize());
		getContext().getOriginTables().addAll(getOriginTables());
		getContext().setClearSrcTablesData(isClearSrcTablesData());
		getContext().setExportMode(getExportMode());
		getContext().setSrcJdbcProperties(getSrcJdbcProperties());
		getContext().setTargetJdbcProperties(getTargetJdbcProperties());
		getContext().setExprotTables(Collections.unmodifiableList(getExportTables()));
		getContext().setSrcTableMetaData(Collections.unmodifiableMap(getSrcTableMetaData()));
		getContext().setTargetTableMetaData(Collections.unmodifiableMap(getTargetTableMetaData()));
		getContext().setTableAliasMapping(Collections.unmodifiableMap(getTableAliasMapping()));
		getContext().setSrcDbCaseSensitive(isSrcDbCaseSensitive());
		getContext().setTargetDbCaseSensitive(isTargetDbCaseSensitive());
		getContext().setSrcExporter(getSrcExporter());
		getContext().setTargetExporter(getTargetExporter());
		getContext().setSrcSchema(getSrcSchema());
		getContext().setTargetSchema(getTargetSchema());
		getContext().setCorePoolSize(getCorePoolSize());
		getContext().setMaxPoolSize(getMaxPoolSize());
		getContext().setParallelThreadSize(getParallelThreadSize());
		getContext().getWhereMapping().putAll(getWhereMapping());
		getContext().setDefaultWhere(getDefaultWhere());
	}

	public void setCommandLine(String[] args) {
		this.args = args;
	}
	
	public String[] getCommandLine() {
		return this.args;
	}
	
	public ExporterContext getContext() {
		return context;
	}

	public void setLoader(ClassLoader loader) {
		this.loader = loader;
	}

	public ClassLoader getLoader() {
		return loader;
	}

	public ExportMode getExportMode() {
		return exportMode;
	}

	public void setExportMode(ExportMode exportMode) {
		this.exportMode = exportMode;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
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

	public String getDefaultWhere() {
		return defaultWhere;
	}

	public void setDefaultWhere(String defaultWhere) {
		this.defaultWhere = defaultWhere;
	}

	public String getDefaultPostSQL() {
		return defaultPostSQL;
	}

	public void setDefaultPostSQL(String defaultPostSQL) {
		this.defaultPostSQL = defaultPostSQL;
	}

	public String getSrcSchema() {
		return srcSchema;
	}

	public void setSrcSchema(String srcSchema) {
		this.srcSchema = srcSchema;
	}

	public String getTargetSchema() {
		return targetSchema;
	}

	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}

	/*public boolean isClearTableData() {
		return clearTableData;
	}

	public void setClearTableData(boolean clearTableData) {
		this.clearTableData = clearTableData;
	}*/

	public Set<String> getOriginTables() {
		return originTables;
	}

	public void setOriginTables(Set<String> originTables) {
		this.originTables = originTables;
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

	public Set<String> getSrcTables() {
		return srcTables;
	}

	public void setSrcTables(Set<String> srcTables) {
		this.srcTables = srcTables;
	}

	public Set<String> getTargetTables() {
		return targetTables;
	}

	public void setTargetTables(Set<String> targetTables) {
		this.targetTables = targetTables;
	}

	public Map<String, String> getWhereMapping() {
		return whereMapping;
	}

	public void setWhereMapping(Map<String, String> whereMapping) {
		this.whereMapping = whereMapping;
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

	public Properties getCommonProperties() {
		return commonProperties;
	}

	public void setCommonProperties(Properties commonProperties) {
		this.commonProperties = commonProperties;
	}

	public boolean isClearSrcTablesData() {
		return clearSrcTablesData;
	}

	public void setClearSrcTablesData(boolean clearSrcTablesData) {
		this.clearSrcTablesData = clearSrcTablesData;
	}

	public List<Triple<String,String,String>> getExportTables() {
		return exportTables;
	}

	public void setExportTables(List<Triple<String,String,String>> exportTables) {
		this.exportTables = exportTables;
	}
	
	private String getSrcTable(String originTable) {
		return isSrcDbCaseSensitive() ? originTable : originTable.toUpperCase();
	}
	
	private String getTargetTable(String originTable) {
		return isTargetDbCaseSensitive() ? originTable : originTable.toUpperCase();
	}
	
	//support table alias.
	public TableMetaData getTargetTableMetaData(String sourceTable, String targetTable) {
		TableMetaData md = getTargetTableMetaData().get(sourceTable + "-->" + targetTable);
		return md == null ? getTargetTableMetaData().get(targetTable) : md;
	}

	public Properties getMappingProperties() {
		return mappingProperties;
	}

	public void setMappingProperties(Properties mappingProperties) {
		this.mappingProperties = mappingProperties;
	}

	public Map<String, Map<String,Object>> getMappingJson() {
		return mappingJson;
	}

	public void setMappingJson(Map<String, Map<String,Object>> mappingJson) {
		this.mappingJson = mappingJson;
	}

	public String getExportSqlFile() {
		return exportSqlFile;
	}

	public void setExportSqlFile(String exportSqlFile) {
		this.exportSqlFile = exportSqlFile;
	}

	public Properties getDatabaseProperties() {
		return databaseProperties;
	}

	public void setDatabaseProperties(Properties databaseProperties) {
		this.databaseProperties = databaseProperties;
	}

	public Map<String, Set<String>> getTableAliasMapping() {
		return tableAliasMapping;
	}

	public void setTableAliasMapping(Map<String, Set<String>> tableAliasMapping) {
		this.tableAliasMapping = tableAliasMapping;
	}
}
