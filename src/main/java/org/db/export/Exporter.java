package org.db.export;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Exporter {
	private static final String CONST_YES = "yes";

	private static final String CONST_JDBC_PASSWD = "jdbc.passwd";

	private static final String CONST_JDBC_USERNAME = "jdbc.username";

	private static final String CONST_JDBC_URL = "jdbc.url";

	private static final String CONST_JDBC_DRIVER_CLASSNAME = "jdbc.driverClassName";

	private static final String CONST_TABLES = "TABLES";

	private static final String CONST_POST_SQL = "POST_SQL";

	private static final String CONST_DEFAULT_WHERE = "DEFAULT_WHERE";

	private static final String CONST_PARALLEL_SIZE = "PARALLEL_SIZE";

	private static final String CONST_BATCH_SIZE = "BATCH_SIZE";
	
	private static final String CONST_CLEAR_DATA = "CLEAR_DATA";
	
	private static final String CONST_SRC_SCHEMA = "SRC_SCHEMA";
	
	private static final String CONST_DEST_SCHEMA = "DEST_SCHEMA";
	
	private static final String CONST_EXPORT_MODE = "EXPORT_MODE";
	
	private static final String CONST_DEFAULT_UNIQUE_COLS = "DEFAULT_UNIQUE_COLS";
	
	private static final String CONST_CORE_POOL_SIZE = "CORE_POOL_SIZE";
	
	private static final String CONST_MAX_POOL_SIZE = "MAX_POOL_SIZE";
	
	//是否在进行导入操作前，将目标数据库中的表都清空.
	private static boolean CLEAR_DATA = false;
	
	//使用的数据导出模式：全量或者增量；默认使用全量模式
	private static ExportMode exportMode = ExportMode.FULL;

	//需要导出的数据库表；
	private static Set<String> tables = null;
	
	private static Set<String> exprotTables = null;
	
	private static Map<String,String> originTablesMapping = null;
	
	private static String DEFAULT_WHERE = "ORDER BY ID";
	
	//每个表数据导出完毕后需要执行的sql.
	private static String POST_SQL = null;
	
	//目标数据库连接信息
	private static Properties DEST_DB = null;
	
	private static String destSchema = null;
	
	private static Map<String,TableMetaData> destTables = null;
	
	//common configuration.
	private static Properties COMMON = null;
	
	//数据导出目标数据库的类型
	private static DBType destDB = null;
	
	//源数据库连接信息
	private static Properties SRC_DB = null;
	
	private static String srcSchema = null;
	
	private static Map<String,TableMetaData> srcTables = null;
	
	private static Map<String, String> srcWhere = new HashMap<String, String>();
	
	//数据导出源数据库类型
	private static DBType srcDB = null;
	
	private static int BATCH_SIZE = 500;
	
	private static int PARALLEL_SIZE = 10;
	
	private static int CORE_POOL_SIZE = 10;
	
	private static int MAX_POOL_SIZE = 300;
	
	private static ExecutorService executor = null;
	
	//数据导出模式
	private static enum ExportMode {
		FULL("FULL")/*全量*/, INC("INC")/*增量*/;

		private String name = null;

		ExportMode(String name) {
			setName(name);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
		
		public static ExportMode getByName(String name) {
			ExportMode[] values = ExportMode.values();
			for(ExportMode value : values) {
				if(value.getName().equalsIgnoreCase(name)) {
					return value;
				}
			}
			
			return null;
		}
	}
	
	//所有支持的数据库类型;
	private static enum DBType {
		MySQL("mysql"), Oracle("oracle");
		
		private String name = null;
		
		DBType(String name) {
			setName(name);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
	
	/**
	 * 数据库元数据的状态.
	 * 
	 * @author Charles.
	 *
	 */
	public static enum MetaDataStatus {
		NORMAL,			
		ONLY_IN_SRC,	//只存在于源数据库中
		ONLY_IN_DEST;   //只存在与目标数据库中
	}
	
	//表结构列元数据
	private static class ColumnMetaData {
		public String name;
		
		public int type = Types.INTEGER;
		
		//是否可以为null.
		public boolean isNullable = false;
		
		//列存在的默认值
		public String defaultValue = null;
		
		@SuppressWarnings("unused")
		public int size = -1; //长度
		
		@SuppressWarnings("unused")
		public int digits = -1; //小数点位数
		
		public  MetaDataStatus status = MetaDataStatus.NORMAL;
	}
	
	//表结构元数据
	private static class TableMetaData {
		public String name = null;
		
		public  MetaDataStatus status = MetaDataStatus.NORMAL;
		
		public Map<String, ColumnMetaData> columnMapping = null;
		
		public List<ColumnMetaData> orderedCols = null; //排好序的列
		
		public List<ColumnMetaData> orderedUniqueCols = new ArrayList<ColumnMetaData>(0);//增量更新时用于update条件的列集合
		
		public Map<String, ColumnMetaData> uniqueMapping = new HashMap<String, ColumnMetaData>(0, 1F);
		
		public Map<String, ColumnMetaData> columnAllMapping = null;
		
		public List<ColumnMetaData> orderedAllCols = null;
	}
	
	//tuple对象
	private static class Tuple<V1,V2> {
		public V1 value1 = null;
		
		public V2 value2 = null;
	}
	
	//triple对象
	private static class Triple<V1,V2,V3> {
		public V1 value1 = null;
		
		public V2 value2 = null;
		
		public V3 value3 = null;
	}
	
	//导出操作执行上下文
	private static InheritableThreadLocal<ExportContext> context
		= new InheritableThreadLocal<ExportContext>();
	private static class ExportContext {
		private static final String CONST_PER_FORMAT = "%.0f";

		private List<String> exportTables = null;
		
		private List<List<String>> cells = null;
		
		private Map<String, Triple<Long/*total rows*/, AtomicLong /*exported rows*/,AtomicLong /*printed rows*/>> tableMapping = null;
		
		public void init(Set<String> tables) {
			exportTables = new ArrayList<String>(tables);
			cells = slice(exportTables, 5);
			
			Collections.sort(exportTables);
			
			tableMapping = new ConcurrentHashMap<String, Triple<Long, AtomicLong, AtomicLong>>(exportTables.size(), 1F);
			for(String table : exportTables) {
				Triple<Long, AtomicLong, AtomicLong> triple = new Triple<Long, AtomicLong, AtomicLong>();
				triple.value1 = -1L;
				triple.value2 = new AtomicLong(0);
				triple.value3 = new AtomicLong(0);
				
				tableMapping.put(table, triple);
			}
		}
		
		public void setTotalRows(String table, Long rows) {
			if(!tableMapping.containsKey(table)) {
				warn("执行上下文中没有导出的表： " + table);
				
				return;
			}
			
			tableMapping.get(table).value1 = rows;
		}
		
		public void inc(String table, Long rows) {
			if(!tableMapping.containsKey(table)) {
				warn("执行上下文中没有导出的表： " + table);
				
				return;
			}
			
			final Triple<Long, AtomicLong, AtomicLong> triple = tableMapping.get(table);
			
			long total = triple.value1;
			if(total < 0) {
				warn("表[" + table + "]没有正确设置tatoal rows，请检查.<br/>");
				
				return;
			}
			
			long oldRows = triple.value3.get();
			long newRows = triple.value2.addAndGet(rows);
			
			//每10%进度打印一次，或者已经100%
			if( total > 0 && (((double)(newRows - oldRows))/total * 100 >= 10
					|| newRows / total == 1)) {
				triple.value3.set(triple.value2.get());
				
				print();
			}
		}
		
		private void print() {
			StringBuilder sb = new StringBuilder("执行进度\n");
			
			sb.append("\t");
			for(List<String> cell : cells) {
				for (String table : cell) {
					Triple<Long, AtomicLong, AtomicLong> tuple = tableMapping.get(table);
					double per = 0;
					if(tuple.value1 > 0) { //total number has been seted or is not null;
						per = (double) (tuple.value2.get()) / tuple.value1 * 100;
					} else if(tuple.value1 == 0) {
						per = 100;
					}
					
					sb.append(table).append("：").append(String.format(CONST_PER_FORMAT, per)).append("%, ");
				}
				sb.setLength(sb.length() - 2);
				sb.append("\n\t");
			}
			sb.setLength(sb.length() - 1);
			
			info(sb);
		}
	}
	
	private Exporter() {
		;
	}
	
	public static void main(String[] args) {
		if(tables == null || tables.size() <= 0
				|| DEST_DB.size() <= 0 || SRC_DB.size() <= 0) {
			return;
		}
		
		//解析表结构
		parseTableMetaData();
		
		//解析命令行参数
		parseCommandLine(args);
		
		//打印配置信息；
		printConfigInfo();
		
		//confirm
		if(!confirm("请确认是否进行数据导出操作（yes/no)：",
				"正在进行导出操作，请耐心等待......",
				"输入不是" + CONST_YES + "，确认不进行导出.")) {
			return;
		}
		
		//清理数据
		if( exportMode == ExportMode.FULL &&
				CLEAR_DATA &&
				confirm("请确认是否进行数据清理操作（yes/no)：",
						"正在进行清理操作，请耐心等待......",
						"输入不是" + CONST_YES + "，确认不进行数据清理.")) {
			clearData();
		}
		
		//数据导出；
		long start = System.currentTimeMillis();
		exportData();
		info("导出所有数据(" + exprotTables.size() + "个表)耗时："
				+ ((System.currentTimeMillis() - start)/1000/60) + " 分钟");
		executor.shutdown();
	}
	
	private static void parseCommandLine(final String[] args) {
		//解析命令行清理参数
		parseClearData(args);
	}

	private static void parseClearData(final String[] args) {
		if(args != null && args.length > 0) {
			boolean clearData = false;
			try {
				clearData = Boolean.parseBoolean(args[0]);
			} catch (Exception e) {
				warn("命令行第一个参数应该是boolean类型，表示是否需要清理目标数据库中的数据.");
			}
			
			CLEAR_DATA = clearData;
		}
	}

	private static boolean confirm(String message, String yesMessage, String noMessage) {
		System.out.print(message);
		
		boolean yes = false;
		try {
			byte[] values = new byte[10];
			int num = System.in.read(values);
			
			String value = null;
			if(num <= 0) {
				value = "";
			} else {
				value = new String(values, 0 , num);
			}
			if(CONST_YES.equals(value.trim())) {
				yes = true;
			}
		} catch (IOException e) {
			yes = false;
		}
		
		if(yes) {
			info(yesMessage);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				; // ignore;
			}
		} else {
			info(noMessage);
		}
		
		return yes;
	}

	private static void printConfigInfo() {
		StringBuilder info = new StringBuilder("数据库导出配置信息：\n");
		
		//数据库类型
		info.append("\t源数据库类型：     " + srcDB.name).append("，schema = ").append(srcSchema).append("\n");
		info.append("\t目标数据库类型： " + destDB.name).append("，schema = ").append(destSchema).append("\n");
		
		//导出模式
		info.append("\t数据导出模式： " + exportMode).append("\n");
		
		//预处理
		info.append("\t导出前清空目标数据库中需要导入表的数据： " + CLEAR_DATA).append("\n");
		
		//执行属性
		info.append("\t导出数据操作特性\n").
			 append("\t\t并发线程池最小活动线程数：").append(CORE_POOL_SIZE).append("\n").
			 append("\t\t并发线程池最大活动线程数：").append(MAX_POOL_SIZE).append("\n").
			 append("\t\t源数据获取数据每页大小：").append(BATCH_SIZE).append("\n").
			 append("\t\t每个导出表并发线程数量：").append(PARALLEL_SIZE).append("\n");
		
		info.append("\t每个表导出完毕后需要执行的SQL：").append((POST_SQL == null ? "无" : POST_SQL)).append("\n");
		
		List<TableMetaData> tables = new ArrayList<TableMetaData>(Exporter.tables.size());
		List<TableMetaData> onlyInSrc = new ArrayList<TableMetaData>(Exporter.tables.size());
		List<TableMetaData> onlyInDest = new ArrayList<TableMetaData>(Exporter.tables.size());
		for(String table : Exporter.tables) {
			TableMetaData srcMeta = srcTables.get(table);
			TableMetaData destMeta = destTables.get(table);
			if(srcMeta != null && destMeta != null) {
				tables.add(srcMeta);
			} else if (srcMeta != null && destMeta == null) {
				onlyInSrc.add(srcMeta);
			} else if (srcMeta == null && destMeta != null) {
				onlyInDest.add(destMeta);
			}
		}
		info.append("\t导出的表信息\n");
		
		printExportTableInfo(info, "需要导出的表：", tables);
		
		printExportTableInfo(info, "只存在源数据库的表（不导出）：", onlyInSrc);
		
		printExportTableInfo(info, "只存在目标数据库的表（不导出）：", onlyInDest);
		
		info(info);
	}
	
	private static void printExportTableInfo(StringBuilder buffer, String title, List<TableMetaData> tables) {
		final String spaces = "    ";
		buffer.append("\t").append(spaces).append(title);
		if(tables == null || tables.size() <= 0) {
			buffer.append("无\n");
			
			return;
		}
		
		buffer.append("\n\t").append(spaces).append(spaces);
		List<List<TableMetaData>> cells = slice(tables, 5);
		for(List<TableMetaData> cell : cells) {
			for(TableMetaData table : cell) {
				buffer.append(table.name).append(",");
			}
			buffer.setLength(buffer.length() - 1);
			buffer.append("\n\t").append(spaces).append(spaces);
		}
		buffer.setLength(buffer.length() - spaces.length() * 2 - 2);
		buffer.append("\n");
	}

	private static void clearData() {
		try {
			Connection con = destCon();
			con.setAutoCommit(false);
			
			for (String table : Exporter.exprotTables) {
				if(destTables.get(table).status != MetaDataStatus.NORMAL) {
					warn("表[" + table + "]只存在于目标数据库中，不进行清理.");
					
					continue;
				}
				
				Statement stat = con.createStatement();
				int i = stat.executeUpdate("DELETE FROM " 
						+ destSchema + "."
						+ (destDB == DBType.MySQL ? originTablesMapping.get(table) : table));
				con.commit();
				stat.close();
				
				info("清理目标数据库[" + destDB.getName() + "]表[" + table + "],总删除数据条数：" + i);
			}
		} catch (SQLException e) {
			throw new RuntimeException("清理目标数据库[" + destDB.getName() + "]数据库数据失败.", e);
		}
	}

	private static void exportData() {
		Thread[] ts = new Thread[exprotTables.size()];
		ExportContext context = new ExportContext();
		context.init(exprotTables);
		Exporter.context.set(context);
		
		int i = 0;
		for(final String table: exprotTables) {
			if(!destTables.containsKey(table)){
				warn(destDB.getName() + "数据库没有表：" + table);
				continue;
			}
			
			if(!srcTables.containsKey(table)){
				warn(srcDB.getName()+"数据库没有表：" + table);
				continue;
			}
			
			ts[i++] = new Thread(new Runnable() {
				@Override
				public void run() {
					exportTableDataParalleled(table);
//					exportTableData(table);
					
					info("表[" + table + "]已经导出完毕.");
				}
			});
		}
		
		for(Thread t : ts) {
			if(t != null) {
				t.start();
			}
		}
		
		for(Thread t : ts) {
			if(t != null && t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void exportTableDataParalleled(final String table){
		TableMetaData destTableMetaData = destTables.get(table);
		if(destTableMetaData.status != MetaDataStatus.NORMAL) {
			warn("表[" + table + "]在源数据库中不存在，不进行数据导出.");
			
			return;
		}
		
		TableMetaData srcTableMetaData = srcTables.get(table);
		if(srcTableMetaData.status != MetaDataStatus.NORMAL) {
			warn("表[" + table + "]在目标数据库中不存在，不进行数据导出.");
			
			return;
		}
		
		String select = genSelect(table, srcTableMetaData.orderedAllCols);
		String saveOrUpdate = null;
		switch(exportMode) {
		case FULL:
			saveOrUpdate = genInsert(table, destTableMetaData.orderedAllCols);
			break;
		case INC:
			saveOrUpdate = genUpdate(table, destTableMetaData.orderedCols
					, destTableMetaData.orderedUniqueCols);
			break;
		default:
			throw new RuntimeException("请正确配置数据库导致模式：EXPORT_MODE=FULL|INC.");
		}
		
		//
		final List<Tuple<Connection, PreparedStatement>> parallelSrcDB = 
			new ArrayList<Tuple<Connection, PreparedStatement>>(PARALLEL_SIZE);
		final List<Tuple<Connection, PreparedStatement>> parallelDestDB = 
			new ArrayList<Tuple<Connection, PreparedStatement>>(PARALLEL_SIZE);
		
		
		try {
			for(int i = 0 ; i < PARALLEL_SIZE; ++i) {
				Tuple<Connection, PreparedStatement> srcTuple = 
					new Tuple<Connection, PreparedStatement>();
				Tuple<Connection, PreparedStatement> destTuple = 
					new Tuple<Connection, PreparedStatement>();
				
				srcTuple.value1 = srcCon();
				srcTuple.value2 = srcTuple.value1.prepareStatement(select);
				parallelSrcDB.add(srcTuple);
				
				destTuple.value1 = destCon();
				destTuple.value1.setAutoCommit(false);
				destTuple.value2 = destTuple.value1.prepareStatement(saveOrUpdate);
				parallelDestDB.add(destTuple);
			}
			
			do {
				//获取总页数；
				long total = getTotalSize(table);
				if(total < 0) {
					break;
				}
				context.get().setTotalRows(table, total);
				
				//pagination;
				List<List<Long>> cells = slice(pagination(total, BATCH_SIZE), PARALLEL_SIZE);
				for(List<Long> cell : cells) {
					
					List<Future<Boolean>> tasks = new ArrayList<Future<Boolean>>(PARALLEL_SIZE);
					for (int i = 0; i < cell.size(); i++) {
						final Tuple<Connection, PreparedStatement> srcTuple = parallelSrcDB.get(i);
						final Tuple<Connection, PreparedStatement> destTuple = parallelDestDB.get(i);
						
						final long index = cell.get(i);
						
						FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
							@Override
							public Boolean call() throws Exception {
								return exportTableData(table, srcTuple.value2, destTuple.value2, index);
							}
						});
						
						tasks.add(futureTask);
						executor.submit(futureTask);
					}
					
					int i = 0;
					for(Future<Boolean> future : tasks) {
						final Tuple<Connection, PreparedStatement> srcTuple = parallelSrcDB.get(i);
						final Tuple<Connection, PreparedStatement> destTuple = parallelDestDB.get(i);
						++i;
						boolean updated = false;
						try {
							updated = future.get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
						
						srcTuple.value2.clearParameters();
						destTuple.value2.clearParameters();
						if(updated) {
							destTuple.value1.commit();
						} else {
							destTuple.value1.rollback();
						}
					}
				}
				
				//post sql execute;
				if(POST_SQL != null) {
					String postSql = POST_SQL.replace("#table#", table);
					
					Connection con = null;
					Statement stat = null;
					try {
						con = destCon();
						con.setAutoCommit(false);
						stat = con.createStatement();
						stat.execute(postSql);
						con.commit();
						
						info("在目标数据库表[" + table + "]成功执行Post SQL.");
					} catch (Exception e) {
						con.rollback();
						warn("在目标数据库表[" + table + "]执行Post SQL失败.");
					} finally {
						closeQuietly(stat);
						closeQuietly(con);
					}
				}
			} while (false);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			for(Tuple<Connection, PreparedStatement> tuple : parallelSrcDB) {
				if(tuple != null) {
					closeQuietly(tuple.value1);
					closeQuietly(tuple.value2);
				}
			}
			
			for(Tuple<Connection, PreparedStatement> tuple : parallelDestDB) {
				if(tuple != null) {
					closeQuietly(tuple.value1);
					closeQuietly(tuple.value2);
				}
			}
		}
	}
	
	public static <T> List<List<T>> slice(List<T> pms, final int size) {
		List<List<T>> rtn = new ArrayList<List<T>>(1);
		if(pms == null || pms.size() <= 0)
			return rtn;
		if(size <= 0) {
			rtn.add(pms);
			
			return rtn;
		}
		
		if(pms.size() <= size) {
			rtn.add(pms);
			return rtn;
		}
		
		rtn = new ArrayList<List<T>>(pms.size() / size + 1);
		for(int i = 0; i < pms.size();) {
			List<T> cell = new ArrayList<T>(size);
			for(int j = 0; j < size && i < pms.size(); j++, i++) {
				cell.add(pms.get(i));
			}
			
			rtn.add(cell);
		}
		
		return rtn;
	}
	
	private static List<Long> pagination(long total, int pageSize) {
		int pageNum = (int)(total / pageSize + 1);
		
		List<Long> pages = new ArrayList<Long>(pageNum);
		for(int i = 0 ; i < pageNum; ++i) {
			pages.add((long)(i*pageSize + 1));
		}
		
		return pages;
	}
	
	private static long getTotalSize(String table) {
		Connection con = null;
		Statement stat = null;
		
		long total = -1;
		try {
			con = srcCon();
			stat = con.createStatement();
			
			ResultSet set = stat.executeQuery("SELECT COUNT(1) FROM " 
					+ srcSchema + "."
					+ (srcDB == DBType.MySQL ? originTablesMapping.get(table) : table));
			if(set != null && set.next()) {
				total = set.getLong(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(con);
			closeQuietly(stat);
		}
		
		return total;
	}

	/*@SuppressWarnings("unused")
	private static void exportTableData(String table) {
		TableMetaData destTableMetaData = destTables.get(table);
		if(destTableMetaData.status != MetaDataStatus.NORMAL) {
			warn("表[" + table + "]在源数据库中不存在，不进行数据导出.");
			
			return;
		}
		
		TableMetaData srcTableMetaData = srcTables.get(table);
		if(srcTableMetaData.status != MetaDataStatus.NORMAL) {
			warn("表[" + table + "]在目标数据库中不存在，不进行数据导出.");
			
			return;
		}
		
		String select = genSelect(table, srcTableMetaData.orderedCols);
		String insert = genInsert(table, destTableMetaData.orderedCols);
		Connection srcCon = null;
		Connection destCon = null;
		PreparedStatement srcStat = null;
		PreparedStatement destStat = null;
		try {
			srcCon = srcCon();
			srcStat = srcCon.prepareStatement(select);
			
			destCon = destCon();
			destCon.setAutoCommit(false);
			destStat = destCon.prepareStatement(insert);
			
			int from = 1; //oracle rowid从1开始；
			while(true){
				ResultSet srcTableData = fetchData(srcStat, from , BATCH_SIZE);
				boolean updated = insertData(table, srcTableData, srcTableMetaData, destStat, destTableMetaData);
				destCon.commit();
				
				from += BATCH_SIZE;
				
				if(!updated) {
					break;
				}
			}
			
			//post sql execute;
			if(POST_SQL != null) {
				String postSql = POST_SQL.replace("#table#", table);
				destStat.execute(postSql);
				destCon.commit();
				
				info("表[" + table + "]Post SQL成功执行.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(srcCon);
			closeQuietly(srcStat);
			closeQuietly(destCon);
			closeQuietly(destStat);
		}
	}*/
	
	private static boolean exportTableData(String table, PreparedStatement srcStat
			, PreparedStatement destStat, long from) {
		try {
			TableMetaData destTable = destTables.get(table);
			TableMetaData srcTable = srcTables.get(table);
			
			ResultSet srcTableData = fetchData(srcStat, from , BATCH_SIZE);
			boolean updated = saveOrUpdate(table, srcTableData, srcTable, destStat, destTable);
			
			return updated;
		} catch (SQLException e) {
			System.out.println(">>>>>>>>>>>>>>>>>>error table: " + table);
			e.printStackTrace();
		}
		
		return false;
	}

	private static boolean saveOrUpdate(String table, ResultSet srcData,
			TableMetaData srcMetaData
			, PreparedStatement destStat
			, TableMetaData destMetaData) throws SQLException {
		
		int srcDataLen = 0;
		while(srcData.next()) {
			int index = 1;
			++srcDataLen; //Oracle不能批量操作时返回更新条目数
			for(ColumnMetaData col : destMetaData.orderedAllCols){
				if(col.status != MetaDataStatus.NORMAL) {
					continue;
				}
				
				//如果目标值为null;
				if(srcData.getObject(col.name) == null) {
					destStat.setNull(index, col.type);
					++index;
					continue;
				}
				
				switch(srcMetaData.columnAllMapping.get(col.name).type) {
				case Types.ARRAY:
					//TODO: we need it?
					break;
				case Types.BIGINT:
					destStat.setLong(index, srcData.getLong(col.name));
					break;
				case Types.BIT:
					destStat.setByte(index, srcData.getByte(col.name));
					break;
				case Types.BLOB:
					destStat.setBytes(index, srcData.getBytes(col.name));
					break;
				case Types.BOOLEAN:
					destStat.setBoolean(index, srcData.getBoolean(col.name));
					break;
				case Types.CHAR:
					destStat.setString(index, srcData.getString(col.name));
					break;
				case Types.CLOB:
					destStat.setString(index, srcData.getString(col.name));
					break;
				case Types.DATALINK:
					//TODO: we need it?
					break;
				case Types.DATE:
					destStat.setDate(index, srcData.getDate(col.name));
					break;
				case Types.DECIMAL:
					destStat.setBigDecimal(index, srcData.getBigDecimal(col.name));
					break;
				case Types.DISTINCT:
					//TODO: we need it?
					break;
				case Types.DOUBLE:
					destStat.setDouble(index, srcData.getDouble(col.name));
					break;
				case Types.FLOAT:
					destStat.setFloat(index, srcData.getFloat(col.name));
					break;
				case Types.INTEGER:
					destStat.setInt(index, srcData.getInt(col.name));
					break;
				case Types.JAVA_OBJECT:
					//TODO: we need it?
					break;
				case Types.LONGNVARCHAR:
					destStat.setNString(index, srcData.getNString(col.name));
					break;
				case Types.LONGVARBINARY:
					destStat.setBytes(index, srcData.getBytes(col.name));
					break;
				case Types.LONGVARCHAR:
					destStat.setString(index, srcData.getString(col.name));
					break;
				case Types.NCHAR:
					destStat.setNString(index, srcData.getNString(col.name));
					break;
				case Types.NCLOB:
					destStat.setNString(index, srcData.getNString(col.name));
					break;
				case Types.NULL:
					//TODO: we need it?
					break;
				case Types.NUMERIC:
					destStat.setLong(index, srcData.getLong(col.name));
					break;
				case Types.NVARCHAR:
					destStat.setNString(index, srcData.getNString(col.name));
					break;
				case Types.OTHER:
					//TODO: we need it?
					break;
				case Types.REAL:
					destStat.setDouble(index, srcData.getDouble(col.name));
					break;
				case Types.REF:
					//TODO: we need it?
					break;
				case Types.ROWID:
					//TODO: we need it?
					break;
				case Types.SMALLINT:
					destStat.setShort(index, srcData.getShort(col.name));
					break;
				case Types.SQLXML:
					//TODO: we need it?
					break;
				case Types.STRUCT:
					//TODO: we need it?
					break;
				case Types.TIME:
					destStat.setDate(index, srcData.getDate(col.name));
					break;
				case Types.TIMESTAMP:
					destStat.setTimestamp(index, srcData.getTimestamp(col.name));
					break;
				case Types.TINYINT:
					destStat.setShort(index, srcData.getShort(col.name));
					break;
				case Types.VARBINARY:
					destStat.setBytes(index, srcData.getBytes(col.name));
					break;
				case Types.VARCHAR:
					destStat.setString(index, srcData.getString(col.name));
					break;
				default:
					break;
				}
				++index;
			}
			
			destStat.addBatch();
			destStat.clearParameters();
		}
		
		int[] nums = destStat.executeBatch();
		if(nums == null) {
			return false;
		}
		
		int total = getTotalNum(nums, srcDataLen);
		/*info("表[" + table + "]从" + srcDB.getName()
				+ "增量导出数据量到" + destDB.getName() + "：" + total);*/
		
		context.get().inc(table, (long)total);
		return total <= 0 ? false : true;
	}
	
	private static int getTotalNum(int[] nums,int srcDataLen) {
		if(destDB == DBType.Oracle) {
			return srcDataLen;
		}
		
		int total = 0;
		for(int num : nums) {
			total += num < 0 ? 0 : num;
		}
		return total;
	}
	
	private static String genSelect(String table,
			Collection<ColumnMetaData> cols) {
		String sql = null;
		switch(srcDB) {
		case MySQL:
			sql = genSelect4MySQL(table, cols);
			break;
		case Oracle:
			sql = genSelect4Oracle(table, cols);
			break;
		default:
			throw new IllegalArgumentException("不支持的源数据库类型: " + srcDB.getName());
		}
		
		return sql;
	}
	
	private static String genSelect4MySQL(String table,
			Collection<ColumnMetaData> cols) {
		StringBuilder select = new StringBuilder();
		select.append("SELECT ");
		
		StringBuilder colsSb = new StringBuilder();
		for(ColumnMetaData col : cols) {
			if(col.status != MetaDataStatus.NORMAL) {
				continue;
			}
			
			colsSb.append(col.name).append(",");
		}
		colsSb.setLength(colsSb.length() - 1);
		
		select.append(colsSb).append(" FROM ")
			  .append(srcSchema).append(".")
			  .append(originTablesMapping.get(table));
		String custom = srcWhere.get(table);
		String where = custom != null ? custom : DEFAULT_WHERE;
		select.append(" ").append(where);
		select.append(" LIMIT  ?,?");
		
		return select.toString();
	}

	private static String genSelect4Oracle(String table,
			Collection<ColumnMetaData> cols) {
		if(srcTables.get(table).status != MetaDataStatus.NORMAL) {
			return null;
		}
		
		StringBuilder inner = new StringBuilder();
		inner.append("SELECT ");
		
		StringBuilder colsSb = new StringBuilder();
		for(ColumnMetaData col : cols) {
			if(col.status != MetaDataStatus.NORMAL) {
				continue;
			}
			
			colsSb.append(col.name).append(",");
		}
		colsSb.setLength(colsSb.length() - 1);
		
		inner.append(colsSb).append(" FROM ").append(srcSchema).append(".").append(table);
		String custom = srcWhere.get(table);
		String where = custom != null ? custom : DEFAULT_WHERE;
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

	private static String genInsert(String table,
			Collection<ColumnMetaData> cols) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(destSchema).append(".")
		   .append((destDB == DBType.MySQL 
				   ? originTablesMapping.get(table):table)).append("(");
		
		for(ColumnMetaData col : cols) {
			if(col.status != MetaDataStatus.NORMAL) {
				continue;
			}
			
			sb.append(col.name).append(",");
		}
		sb.setLength(sb.length() - 1);
		sb.append(") VALUES (");
		
		for(ColumnMetaData col : cols) {
			if(col.status == MetaDataStatus.NORMAL) {
				sb.append("?,");
			}
		}
		sb.setLength(sb.length() - 1);
		sb.append(")");
		
		return sb.toString();
	}
	
	private static String genUpdate(String table,
			Collection<ColumnMetaData> cols, Collection<ColumnMetaData> uniqueCols) {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ").append(destSchema).append(".")
		  .append((destDB == DBType.MySQL 
				? originTablesMapping.get(table):table)).append(" SET ");
		
		for(ColumnMetaData col : cols) {
			if(col.status != MetaDataStatus.NORMAL) {
				continue;
			}
			
			sb.append(col.name).append(" = ?,");
		}
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE ");
		
		//TODO: update key cols, exclude lob's;
		for(ColumnMetaData col : uniqueCols) {
			if(col.status == MetaDataStatus.NORMAL) {
				sb.append(col.name).append("=? AND ");
			}
		}
		sb.setLength(sb.length() - 5);
		
		return sb.toString();
	}

	private static ResultSet fetchData(PreparedStatement stat
			, long from, long pageSize) throws SQLException{
		if(srcDB == DBType.MySQL) {
			stat.setLong(1, from - 1);
			stat.setLong(2, pageSize);
		} else if(srcDB == DBType.Oracle) {
			stat.setLong(1, from);
			stat.setLong(2, from + pageSize);
		}
		
		return stat.executeQuery();
	}

	/**
	 * 解析“目的数据库”以及“源数据库”的表结构.<br/>
	 * 
	 */
	private static void parseTableMetaData() {
		//“目的数据库”
		Connection con = null;
		try {
			destTables = new HashMap<String, TableMetaData>(tables.size(), 1F);
			con = destCon();
			parseTableMetaData(con, destTables, destDB, destSchema);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(con);
		}
		
		//“源数据库”
		con = null;
		try {
			srcTables = new HashMap<String, TableMetaData>(tables.size(), 1F);
			
			con = srcCon();
			parseTableMetaData(con, srcTables, srcDB, srcSchema);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(con);
		}
		
		checkMetaData();
	}
	
	private static void parseTableMetaData(Connection con, Map<String, TableMetaData> tables
			, DBType dbType, String schema) throws SQLException {
		for (String table : Exporter.tables) {
			ResultSet rs = con.getMetaData().getColumns(
					(dbType == DBType.MySQL? schema : null), 
					(dbType == DBType.MySQL? schema : schema.toUpperCase())
					, (dbType == DBType.MySQL 
						? originTablesMapping.get(table) : table)
					, null);

			readTableMetadata(table, rs, tables, dbType);
			closeQuietly(rs);
		}
	}
	
	private static void checkMetaData() {
		checkMetaData(srcTables, destTables, MetaDataStatus.ONLY_IN_SRC);
		
		checkMetaData(destTables, srcTables, MetaDataStatus.ONLY_IN_DEST);
		
		exprotTables = new HashSet<String>(Exporter.tables.size(), 1F);
		for(String table : Exporter.tables) {
			final TableMetaData srcMeta = srcTables.get(table);
			final TableMetaData destMeta = destTables.get(table);
			
			if(srcMeta != null && destMeta != null) {
				exprotTables.add(table);
			}
			
			if(srcMeta == null && destMeta == null) {
				warn("表[" + table + "]即不在源数据库也不在目标数据库中.");
			}
		}
		
		nullableCheck(destTables);
	}
	
	/**
	 * 目标表中如果有列不在源数据库中，并且设置了不能为nullable的且没有默认值，则提示错误.<br/>
	 * 
	 * @param destTables
	 */
	private static void nullableCheck(Map<String, TableMetaData> destTables) {
		Set<Entry<String, TableMetaData>> entries = destTables.entrySet();
		for(Entry<String, TableMetaData> entry : entries) {
			final String key = entry.getKey();
			final TableMetaData table = entry.getValue();
			
			for(ColumnMetaData col : table.orderedAllCols) {
				if(col.status != MetaDataStatus.NORMAL
						&& !col.isNullable && col.defaultValue == null) {
					throw new RuntimeException("目标数据库表[" + key 
							+ "]有新增列[" + col.name + "]，但是存在NOT NULL约束且没有设置默认值.");
				}
			}
		}
	}

	private static void checkMetaData(Map<String, TableMetaData> src,
			Map<String, TableMetaData> dest, MetaDataStatus toStatus) {
		Set<Entry<String, TableMetaData>> entries = src.entrySet();
		for(Entry<String, TableMetaData> entry : entries) {
			final String key = entry.getKey();
			
			if(!dest.containsKey(key)) {
				entry.getValue().status = toStatus;
				
				continue;
			}
			
			final TableMetaData destTable = dest.get(key);
			final TableMetaData srcTable = entry.getValue();
			for(ColumnMetaData col : srcTable.orderedAllCols) {
				if(!destTable.columnAllMapping.containsKey(col.name)) {
					col.status = toStatus;
				}
			}
		}
	}

	private static void readTableMetadata(String table, ResultSet rs,
			Map<String, TableMetaData> tables, DBType dbType) throws SQLException {
		if(rs == null || !rs.next()) {
			return;
		}
		
		TableMetaData meta = new TableMetaData();
		meta.name = table;
		meta.columnMapping = new HashMap<String, ColumnMetaData>(30, 1F);
		tables.put(table, meta);
		
		do{
			ColumnMetaData column = new ColumnMetaData();
			column.name 		= rs.getString("COLUMN_NAME").toUpperCase();
			column.type 		= rs.getInt("DATA_TYPE");
			column.size 		= rs.getInt("COLUMN_SIZE");
			column.digits 		= rs.getInt("DECIMAL_DIGITS");
			column.defaultValue = rs.getString("COLUMN_DEF");
			
			int nullable 		= rs.getInt("NULLABLE");
			switch(nullable) {
			case DatabaseMetaData.columnNoNulls:
				column.isNullable = false;
				break;
			case DatabaseMetaData.columnNullable:
				column.isNullable = true;
				break;
			case DatabaseMetaData.columnNullableUnknown:
				column.isNullable = true;
				break;
			default:
				column.isNullable = true;
				break;
			}
			
			//nclob for oracle;
			if(dbType == DBType.Oracle) {
				String typeName = rs.getString("TYPE_NAME");
				if(typeName.equalsIgnoreCase("NCLOB")) {
					column.type = Types.NCLOB;
				}
			}
			
			meta.columnMapping.put(column.name, column);
		} while(rs.next());
		
		//parse update unique columns;
		if(exportMode == ExportMode.INC) {
			parsePerTableUnique(meta);
		}
		
		meta.orderedUniqueCols = new ArrayList<ColumnMetaData>(meta.uniqueMapping.values());
		meta.orderedCols = new ArrayList<ColumnMetaData>(meta.columnMapping.values());
		
		meta.columnAllMapping  = new HashMap<String, ColumnMetaData>(meta.columnMapping);
		meta.columnAllMapping.putAll(meta.uniqueMapping);
		meta.orderedAllCols = new ArrayList<ColumnMetaData>(meta.orderedCols);
		meta.orderedAllCols.addAll(meta.orderedUniqueCols);
	}
	
	private static void parsePerTableUnique(TableMetaData meta) {
		final String prefix = "UNIQUE_COLS_";
		
		Set<Object> keys = COMMON.keySet();
		for(String table : tables) {
			for(Object key : keys) {
				String _keyStr = key.toString();
				String keyStr = _keyStr.replace("%", "[a-zA-Z0-9]*");
				Pattern ptn = Pattern.compile(keyStr);
				
				//upper case.
				Matcher matcher = ptn.matcher(prefix + table);
				if(matcher.matches()) {
					parsePerTableUnique(meta, COMMON.getProperty(_keyStr));
					
					break;
				}
				
				//lower case.
				matcher = ptn.matcher(prefix + table.toLowerCase());
				if(matcher.matches()) {
					parsePerTableUnique(meta, COMMON.getProperty(_keyStr));
					
					break;
				}
				
				//origin.
				matcher = ptn.matcher(prefix + originTablesMapping.get(table));
				if(matcher.matches()) {
					parsePerTableUnique(meta, COMMON.getProperty(_keyStr));
					
					break;
				}
				
				//default
				if(meta.uniqueMapping == null
						|| meta.uniqueMapping.isEmpty()) {
					parsePerTableUnique(meta, COMMON.getProperty(CONST_DEFAULT_UNIQUE_COLS));
				}
			}
		}
	}

	private static void parsePerTableUnique(TableMetaData meta, String property) {
		if(property == null || property.isEmpty()) {
			return;
		}
		
		meta.uniqueMapping = new HashMap<String, ColumnMetaData>();
		StringTokenizer tokens = new StringTokenizer(property.trim(), ",");
		while(tokens.hasMoreElements()) {
			String column = tokens.nextToken();
			if(column != null && !(column = column.trim()).isEmpty()) {
				column = column.toUpperCase();
				meta.uniqueMapping.put(column, meta.columnMapping.get(column));
				meta.columnMapping.remove(column);
			}
		}
	}

	private static Connection destCon(/*DBType type*/) {
		try {
			return con(DEST_DB);
		} catch (SQLException e) {
			throw new IllegalArgumentException("请正确配置目标数据库JDBC连接信息.", e);
		}
	}
	
	private static Connection srcCon(/*DBType type*/) {
		try {
			return con(SRC_DB);
		} catch (SQLException e) {
			throw new IllegalArgumentException("请正确配置源数据库JDBC连接信息.",e);
		}
	}

	private static Connection con(Properties prop) throws SQLException{
		return DriverManager.getConnection(
				prop.getProperty(CONST_JDBC_URL)
				, prop.getProperty(CONST_JDBC_USERNAME)
				, prop.getProperty(CONST_JDBC_PASSWD));
	}
	
	//初始化系统信息
	static {
		ClassLoader loader = Exporter.class.getClassLoader();
		if(loader == null) {
			loader = Thread.currentThread().getContextClassLoader();
		}
		
		if(loader == null) {
			throw new IllegalStateException("程序没有正常启动，找不到正确的ClassLoader.");
		}
		
		
		COMMON = fetchProperties(loader, "config/commons.properties");
		//导出时每页大小
		String exportMode = getProperty(COMMON, CONST_EXPORT_MODE);
		if(exportMode != null) {
			ExportMode mode = ExportMode.getByName(exportMode);
			Exporter.exportMode = mode == null ? ExportMode.FULL : mode;
		}
		
		//导出时每页大小
		String batchSize = getProperty(COMMON, CONST_BATCH_SIZE);
		if(batchSize != null) {
			try {
				BATCH_SIZE = Integer.parseInt(batchSize);
			} catch (NumberFormatException e) {
				warn("批量操作的大小必须是数字，使用默认值：" + BATCH_SIZE);
			}
		}
		
		//导出时单个表并发线程数大小
		String corePoolSize = getProperty(COMMON, CONST_CORE_POOL_SIZE);
		if(corePoolSize != null) {
			try {
				CORE_POOL_SIZE = Integer.parseInt(corePoolSize);
			} catch (NumberFormatException e) {
				warn("并发线程池的最小活动线程数，使用默认值：：" + CORE_POOL_SIZE);
			}
		}
		
		String maxPoolSize = getProperty(COMMON, CONST_MAX_POOL_SIZE);
		if(maxPoolSize != null) {
			try {
				MAX_POOL_SIZE = Integer.parseInt(maxPoolSize);
			} catch (NumberFormatException e) {
				warn("并发线程池的最大活动线程数，使用默认值：" + MAX_POOL_SIZE);
			}
		}
		
		//executor pool params;
		String parallelSize = getProperty(COMMON, CONST_PARALLEL_SIZE);
		if(parallelSize != null) {
			try {
				PARALLEL_SIZE = Integer.parseInt(parallelSize);
			} catch (NumberFormatException e) {
				warn("每个表并发导出数据的线程数必须是数字，使用默认值：" + PARALLEL_SIZE);
			}
		}
		
		String defaultWhere = getProperty(COMMON, CONST_DEFAULT_WHERE); //DEFAULT WHERE STATEMENTS;
		if(defaultWhere != null) {
			DEFAULT_WHERE = defaultWhere;
		}
		
		String postSQL = getProperty(COMMON, CONST_POST_SQL);
		if(postSQL != null) {
			POST_SQL = postSQL;
		}
		
		String srcSchema = getProperty(COMMON, CONST_SRC_SCHEMA);
		if(srcSchema != null && (srcSchema = srcSchema.trim()).length() > 0) {
			Exporter.srcSchema = srcSchema;
		}
		
		String destSchema = getProperty(COMMON, CONST_DEST_SCHEMA);
		if(destSchema != null && (destSchema = destSchema.trim()).length() > 0) {
			Exporter.destSchema = destSchema;
		}
		
		String clearData = getProperty(COMMON, CONST_CLEAR_DATA);
		if(clearData != null) {
			try {
				CLEAR_DATA = Boolean.parseBoolean(clearData);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		String values = (String)COMMON.get(CONST_TABLES);
		if(values == null || values.length() == 0) {
			warn("没有需要进行数据导出的表，请配置TABLES参数.");
		}
		
		String[] valueArr = values.split(",");
		tables = new HashSet<String>(valueArr.length, 1F);
		originTablesMapping = new HashMap<String,String>(valueArr.length, 1F);
		for(String value : valueArr) {
			value = value == null ? "" : value.trim();
			if(value.length() > 0) {
				tables.add(value.toUpperCase());
				
				originTablesMapping.put(value.toUpperCase(), value);
			}
		}
		
		//每个表自己的order by条件；
		parsePerTableWhere(COMMON);
		
		DEST_DB = fetchProperties(loader, "config/dest-DataSource.properties");
		
		SRC_DB = fetchProperties(loader, "config/src-DataSource.properties");
		
		try {
			Class.forName(DEST_DB.getProperty(CONST_JDBC_DRIVER_CLASSNAME));
			Class.forName(SRC_DB.getProperty(CONST_JDBC_DRIVER_CLASSNAME));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			
			throw new RuntimeException("需要正确配置“源数据库”和“目标数据库”的驱动程序，参考：jdbc.driverClassName.");
		}
		
		//解析“源数据库”和“目标数据库”的类型
		parseDBType();
		
		//解析数据库shema
		parseSchema();
		
		executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, 60, TimeUnit.SECONDS
				, new LinkedBlockingDeque<Runnable>(100000));
	}
	
	private static void parseSchema() {
		if(srcSchema == null) {
			if(srcDB == DBType.MySQL) {
				srcSchema = parseMySQLSchema(SRC_DB);
			} else if(srcDB == DBType.Oracle) {
				srcSchema = parseOracleSchema(SRC_DB);
			}
		}
		
		if(destSchema == null) {
			if(destDB == DBType.MySQL) {
				destSchema = parseMySQLSchema(SRC_DB);
			} else if(destDB == DBType.Oracle) {
				destSchema = parseOracleSchema(SRC_DB);
			}
		}
		
		if(srcSchema == null 
				|| (srcSchema = srcSchema.trim()).length() == 0) {
			throw new IllegalArgumentException("源数据库schema不能为空.");
		}
		
		if(destSchema == null 
				|| (destSchema = destSchema.trim()).length() == 0) {
			throw new IllegalArgumentException("目标据库schema不能为空.");
		}
	}
	
	private static String parseMySQLSchema(Properties prop) {
		String schema = null;
		String url = prop.getProperty(CONST_JDBC_URL);
		if(url != null && url.length() > 0) {
			int index = url.lastIndexOf('/');
			int endIndex = url.lastIndexOf('?');
			if(endIndex <= 0) {
				endIndex = url.length();
			}
			schema = url.substring(index + 1, endIndex).toUpperCase();
		}
		return schema;
	}
	
	private static String parseOracleSchema(Properties prop) {
		return prop.getProperty(CONST_JDBC_USERNAME).toUpperCase();
	}
	
	private static String getProperty(Properties prop, String key) {
		String value = prop.getProperty(key);
		if(value != null && value.trim().length() > 0) {
			return value.trim();
		}
		
		return null;
	}
	
	private static void parseDBType() {
		//源数据库
		String name = fetchDBProductName(srcCon());
		srcDB = matchDBType(name);
		if(srcDB == null) {
			throw new IllegalArgumentException("不支持的源数据库类型：" + name);
		}
		
		//目标数据库
		name = fetchDBProductName(destCon());
		destDB = matchDBType(name);
		if(destDB == null) {
			throw new IllegalArgumentException("不支持的目的数据库类型：" + name);
		}
	}
	
	private static DBType matchDBType(String name) {
		if(name == null) {
			return null;
		}
		
		DBType[] types = DBType.values();
		for(DBType type : types) {
			if(name.toLowerCase().indexOf(type.getName()) >= 0) {
				return type;
			}
		}
		
		return null;
	}
	
	private static String fetchDBProductName(Connection con) {
		String name = null;
		
		try {
			DatabaseMetaData meta = con.getMetaData();
			name = meta.getDatabaseProductName();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(con);
		}
		
		return name;
	}

	private static void parsePerTableWhere(Properties prop) {
		final String prefix = "WHERE_";
		
		Set<Object> keys = prop.keySet();
		for(String table : tables) {
			for(Object key : keys) {
				String _keyStr = key.toString();
				String keyStr = _keyStr.replace("%", "[a-zA-Z0-9]*");
				Pattern ptn = Pattern.compile(keyStr);
				
				//upper case.
				Matcher matcher = ptn.matcher(prefix + table);
				if(matcher.matches()) {
					srcWhere.put(table, prop.getProperty(_keyStr));
					
					break;
				}
				
				//lower case.
				matcher = ptn.matcher(prefix + table.toLowerCase());
				if(matcher.matches()) {
					srcWhere.put(table, prop.getProperty(_keyStr));
					
					break;
				}
				
				//origin.
				matcher = ptn.matcher(prefix + originTablesMapping.get(table));
				if(matcher.matches()) {
					srcWhere.put(table, prop.getProperty(_keyStr));
					
					break;
				}
			}
		}
		
	}

	private static Properties fetchProperties(ClassLoader loader, String path) {
		InputStream in = loader.getResourceAsStream(path);
		if(in == null) {
			throw new IllegalArgumentException("请正确配置: " + path);
		}
		
		Properties prop = new Properties();
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(in);
		}
		
		return prop;
	}
	
	private static void closeQuietly(InputStream in) {
		try {
			if(in != null) {
				in.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void closeQuietly(Connection con) {
		try {
			if(con != null) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void closeQuietly(ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void closeQuietly(Statement stat) {
		try {
			if(stat != null) {
				stat.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void warn(Object message) {
		System.out.println("[WARN]" + message);
	}
	
	@SuppressWarnings("unused")
	private static void error(Object message) {
		System.out.println("[ERROR]" + message);
	}
	
	private static void info(Object message) {
		System.out.println("[INFO]" + message);
	}
}
