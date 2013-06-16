package org.db.export.common.runner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.db.export.common.Constants;
import org.db.export.common.ExporterContext;
import org.db.export.common.Iterable;
import org.db.export.common.RunnerException;
import org.db.export.common.log.Logger;
import org.db.export.common.log.LoggerFactory;
import org.db.export.common.runner.type.ExportMode;
import org.db.export.common.type.TableMetaData;
import org.db.export.common.type.Triple;
import org.db.export.common.util.CommonUtils;
import org.db.export.common.util.DataSourceUtils;
import org.db.export.common.util.ProgressUtils;

public abstract class AbstractRunner implements Runner {
	private static final Logger log = LoggerFactory.getLogger(AbstractRunner.class);
	
	private ExporterContext context = null;
	
	@Override
	public final void execute(ExporterContext context) throws RunnerException {
		setContext(context);
		
		if(!confirmContinue()) {
			return;
		}
		
		if(confirmClearTableData()) {
			doClearTableData();
		}
		
		//export table data.
		long start = System.currentTimeMillis();
		
		Set<String> keys = new HashSet<String>();
		for(Triple<String,String,String> triple : getContext().getExprotTables()) {
			keys.add(triple.value2 + "-->" + triple.value3);
		}
		ProgressUtils.getInstance().init(keys);
		
		initExportEnv();
		exportAllTables(getContext().getExprotTables());
		log.info("All table has been exported( " + getContext().getExprotTables().size() + " tables), time consuming: "
				+ ((System.currentTimeMillis() - start)/1000/60) + " minute");
	}
	
	protected void exportAllTables(List<Triple<String, String, String>> exprotTables)  throws RunnerException {
		for(Triple<String,String,String> triple : exprotTables) {
			exportTableData(triple.value2, triple.value3, triple.value1);
		}
	}

	protected void exportTableData(String sourceTable, String targetTable, String originTable) throws RunnerException{
		ExporterContext context = getContext();
		TableMetaData targetTableMetaData = context.getTargetTableMetaData(sourceTable, targetTable);
		TableMetaData srcTableMetaData = context.getSrcTableMetaData().get(sourceTable);
		
		long totalSize = getTotalSize(sourceTable, targetTable, originTable);
		ProgressUtils.getInstance().setTotalRows(sourceTable + "-->" + targetTable, totalSize);

		String where = context.getWhere(sourceTable, targetTable, originTable);
		String select = context.getSrcExporter().genSelectDML(context.getSrcSchemaPrefix(),
				context.getSrcTable(sourceTable), srcTableMetaData.getOrderedCols(), where);
		String updateOrInsert = context.getExportMode() == ExportMode.FULL ? 
				context.getTargetExporter().genInsertDML(context.getTargetSchemaPrefix()
						, context.getTargetTable(targetTable), targetTableMetaData.getOrderedCols())
						: context.getTargetExporter().genUpdateDML(context.getTargetSchemaPrefix()
								, context.getTargetTable(targetTable), targetTableMetaData.getOrderedCols()
								, targetTableMetaData.getOrderedUniqueCols());

		
		Iterable ite = new Iterable(totalSize, context.getPageSize()
				, sourceTable, targetTable, select, updateOrInsert);
		ite.setContext(getContext());
		
		try {
			doExportTableData(ite);
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new RunnerException("export table[" + sourceTable + "-->" + targetTable + "] error.", e);
		} finally {
			for(org.db.export.common.Statement stat : ite.getCached()) {
				stat.unbind();
			}
		}
	}
	
	protected abstract void doExportTableData(Iterable ite)  throws RunnerException;
	
	protected long getTotalSize(String sourceTable, String targetTable, String originTable) {
		Connection con = null;
		Statement stat = null;
		
		long total = -1;
		try {
			con = DataSourceUtils.con(getContext().getSrcJdbcProperties());
			stat = con.createStatement();
			
			String where = context.getWhere(sourceTable, targetTable, originTable);
			String countDML = getContext().getSrcExporter().genCountDML(
					getContext().getSrcSchemaPrefix(), getContext().getSrcTable(sourceTable) , where);
			ResultSet set = stat.executeQuery(countDML);
			if(set != null && set.next()) {
				total = set.getLong(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceUtils.closeQuietly(stat);
			DataSourceUtils.closeQuietly(con);
		}
		
		return total;
	}

	protected abstract void initExportEnv() throws RunnerException;

	protected abstract void doClearTableData() throws RunnerException;
	
	/**
	 * Let teh user to confirm whether to continue export?<br/>
	 * 
	 * @return
	 */
	protected boolean confirmContinue() {
		return CommonUtils.confirm("Please confirm whether continue exporting(yes/no): ",
				"operation is going，please waiting......",
				"Input is '" + Constants.CONST_YES + "', nothing to be done.");
	}
	
	protected boolean confirmClearTableData() {
		return getContext().isClearSrcTablesData() &&
				CommonUtils.confirm("Please confirm whether execute clear data(yes/no): ",
						"operation is going，please waiting......",
						"Input is '" + Constants.CONST_YES + "', nothing to be done.");
	}

	public ExporterContext getContext() {
		return context;
	}

	public void setContext(ExporterContext context) {
		this.context = context;
	}
}
