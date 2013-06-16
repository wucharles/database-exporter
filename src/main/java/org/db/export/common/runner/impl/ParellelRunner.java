package org.db.export.common.runner.impl;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.db.export.common.ExporterContext;
import org.db.export.common.Iterable;
import org.db.export.common.RunnerException;
import org.db.export.common.log.Logger;
import org.db.export.common.log.LoggerFactory;
import org.db.export.common.runner.AbstractRunner;
import org.db.export.common.runner.Runner;
import org.db.export.common.type.TableMetaData;
import org.db.export.common.type.Triple;
import org.db.export.common.util.CommonUtils;
import org.db.export.common.util.DataSourceUtils;

public class ParellelRunner extends AbstractRunner implements Runner{
	private static final Logger log = LoggerFactory.getLogger(ParellelRunner.class);
	
	private ExecutorService executor = null; 
	
	private CompletionService<org.db.export.common.Statement> service = null;
	
	//per table per thread.
	private Thread[] tableThreads = null;

	@Override
	protected void doClearTableData() throws RunnerException {
		Connection con = null;
		try {
			ExporterContext context = getContext();
			con = DataSourceUtils.con(context.getTargetJdbcProperties());
			con.setAutoCommit(false);
			
			for (Triple<String,String,String> triple : context.getExprotTables()) {
				final TableMetaData metaData = context.getTargetTableMetaData(triple.value2, triple.value3);
				
				doClearTableData(con, metaData, triple.value3);
			}
		} catch (Exception e) {
			//e.printStackTrace();
			
			throw new RunnerException("Clear target tables data error.", e);
		} finally {
			DataSourceUtils.closeQuietly(con);
		}
	}
	
	protected void doClearTableData(Connection con, TableMetaData metaData, String tableName) throws Exception {
		Statement stat = con.createStatement();
		String schemaPrefix = getContext().getTargetSchemaPrefix();
		int i = stat.executeUpdate(getContext().getTargetExporter().genClearTableDML(schemaPrefix, tableName));
		con.commit();
		stat.close();
		
		log.info("Clear target database[" + getContext().getTargetExporter().getDbName()
				+ "]table[" + tableName + "],total rows deleted：" + i);
	}
	
	protected void initExportEnv() throws RunnerException {
		if(CommonUtils.isEmpty(getContext().getOriginTables())) {
			return;
		}
		
		initExecutor();
	}
	
	@Override
	protected void exportAllTables(List<Triple<String, String, String>> exprotTables)  throws RunnerException {
		try {
			initTableThreads(exprotTables);
			
			startTableThreads();
			
			waitTableThreads();
		} finally {
			destroyExecutor();
		}
	}

	private void destroyExecutor() {
		getExecutor().shutdown();
	}

	private void waitTableThreads() {
		for(Thread t : getTableThreads()) {
			if(t != null && t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void startTableThreads() {
		for(Thread t  :  getTableThreads()) {
			if(t != null) {
				t.start();
			}
		}
	}

	private void initTableThreads(List<Triple<String, String, String>> exprotTables) {
		List<Thread> ts = new ArrayList<Thread>(exprotTables.size());
		
		for(final Triple<String, String, String> table : exprotTables) {
			createThread(ts,  table.value2,  table.value3, table.value1);
		}
		
		setTableThreads(ts.toArray(new Thread[ts.size()]));
	}
	
	private void createThread(List<Thread> threads, final String sourceTable, final String targetTable, final String originTable) {
		threads.add(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					exportTableData(sourceTable, targetTable, originTable);
				} catch (RunnerException e) {
					e.printStackTrace();
				}
				
				log.info("Table [" + sourceTable + "-->" + targetTable + "] export completed.");
			}
		}));
	}

	private void initExecutor() {
		setExecutor(new ThreadPoolExecutor(getContext().getCorePoolSize(), getContext().getMaxPoolSize()
				, 60, TimeUnit.SECONDS , new LinkedBlockingDeque<Runnable>(100000)));
		
		setService(new ExecutorCompletionService<org.db.export.common.Statement>(getExecutor()));
	}
	
	@Override
	protected void doExportTableData(Iterable ite) throws RunnerException {
		Iterator<org.db.export.common.Statement>  iterator = ite.iterator();
		int parallelSize = getContext().getParallelThreadSize() - 1;
		if(parallelSize <= 0) {
			parallelSize = 1;
		}
		
		int submitNum = 0;
		int returnNum = 0;
		for (int i = 0; i < parallelSize && iterator.hasNext(); i++) {
			final org.db.export.common.Statement stat = iterator.next();
			submit(stat, ite);
			submitNum++;
		}
		
		if(submitNum <= 0) {
			return;
		}
		
		while(iterator.hasNext()) {
			final org.db.export.common.Statement stat = iterator.next();
			submit(stat, ite);
			submitNum++;
			
			try {
				org.db.export.common.Statement statRet = getService().take().get();
				ite.cache(statRet);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				returnNum++;
			}
		};
		
		//handle remainder tasks.
		while(returnNum < submitNum) {
			try {
				org.db.export.common.Statement statRet = getService().take().get();
				ite.cache(statRet);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				returnNum++;
			}
		}
	}
	
	private void submit(final org.db.export.common.Statement stat, final Iterable ite) {
		getService().submit(
				new Callable<org.db.export.common.Statement>() {
					@Override
					public org.db.export.common.Statement call() throws Exception {
						try {
							if(!stat.isBinded()) {
								Connection src = DataSourceUtils.con(getContext().getSrcJdbcProperties());
								Connection target = DataSourceUtils.con(getContext().getTargetJdbcProperties());
								target.setAutoCommit(false);
								stat.bind(src, target);
							}

							stat.exec();
						} catch (Exception e) {
							e.printStackTrace();
							ite.cache(stat);

							throw new RunnerException("run statement exception.", e);
						}

						return stat;
					}
				});
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	public Thread[] getTableThreads() {
		return tableThreads;
	}

	public void setTableThreads(Thread[] tableThreads) {
		this.tableThreads = tableThreads;
	}

	public CompletionService<org.db.export.common.Statement> getService() {
		return service;
	}

	public void setService(CompletionService<org.db.export.common.Statement> service) {
		this.service = service;
	}
}
