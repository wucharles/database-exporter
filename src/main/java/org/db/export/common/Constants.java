package org.db.export.common;

public interface Constants {
	public static final String CONST_YES = "yes";

	public static final String CONST_JDBC_PASSWD = "jdbc.passwd";

	public static final String CONST_JDBC_USERNAME = "jdbc.username";

	public static final String CONST_JDBC_URL = "jdbc.url";

	public static final String CONST_JDBC_DRIVER_CLASSNAME = "jdbc.driverClassName";

	public static final String CONST_TABLES = "TABLES";

	public static final String CONST_DEFAULT_POST_SQL = "DEFAULT_POST_SQL";

	public static final String CONST_DEFAULT_WHERE = "DEFAULT_WHERE";

	public static final String CONST_PARALLEL_THREAD_SIZE = "PARALLEL_THREAD_SIZE";

	public static final String CONST_PAGE_SIZE = "PAGINATION_SIZE";

	public static final String CONST_CLEAR_TABLES_DATA = "CLEAR_TABLES_DATA";

	public static final String CONST_SRC_SCHEMA = "SRC_SCHEMA";

	public static final String CONST_DEST_SCHEMA = "TARGET_SCHEMA";

	public static final String CONST_EXPORT_MODE = "EXPORT_MODE";

	public static final String CONST_DEFAULT_UNIQUE_COLS = "DEFAULT_UNIQUE_COLS";

	public static final String CONST_CORE_POOL_SIZE = "CORE_POOL_SIZE";

	public static final String CONST_MAX_POOL_SIZE = "MAX_POOL_SIZE";
	
	public static final String CONST_SQL_FILE = "export.sql";
	
	public static final String CONST_SQL_FILE_PREFIX = "export-";
	
	public static final String CONST_SQL_FILE_SUFFIX = ".sql";
	
	public static final String CONST_COMMON_PROP_FILE = "config/commons.properties";
	
	public static final String CONST_DATABASE_PROP_FILE = "config/database.properties";
	
	public static final String CONST_MAPPING_PROP_FILE = "config/mapping.properties";
	
	public static final String CONST_SRC_PROP_FILE = "config/src-DataSource.properties";
	
	public static final String CONST_DEST_PROP_FILE = "config/dest-DataSource.properties";
	
	public static final String CONST_DEST_DB_CASE_SENSITIVE = "DEST_DB_CASE_SENSITIVE";
	
	public static final String CONST_SRC_DB_CASE_SENSITIVE = "SRC_DB_CASE_SENSITIVE";
	
	public static final String CONST_EXPORTER_FILE = "META-INF/exporter";
	
	public static final String CONST_WHERE_PREFIX = "WHERE_";
	
	public static final String CONST_UNIQUE_PREFIX = "UNIQUE_COLS_";
	
	public static final String CONST_EXPORT_TABLE_ALIAS = "EXPORT_TABLE_ALIAS";
	
	public static final int DEFAULT_PAGE_SIZE = 500;
	
	public static final int DEFAULT_CORE_POOL_SIZE = 10;
	
	public static final int DEFAULT_MAX_POOL_SIZE = 100;
	
	public static final int DEFAULT_PARALLEL_THREAD_SIZE = 10;
	
	public static final String DEFAULT_WHERE = "ORDER BY ID";
	
	public static boolean DEFAULT_CLEAR_TABLE_DATA = false;
	
	public static boolean DEFAULT_SRC_DB_CASE_SENSITIVE = false;
	
	public static boolean DEFAULT_DEST_DB_CASE_SENSITIVE = false;
}
