package org.db.export.common;

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

import org.db.export.common.impl.StatementImpl;

public class Iterable implements java.lang.Iterable<Statement> {
	private long totalSize = -1L;
	
	private String select = null;
	
	private String updateOrInsert = null;
	
	private int pageSize = -1;
	
	private int index = 1;
	
	private Deque<Statement> cached = new LinkedBlockingDeque<Statement>();
	
	private ExporterContext context = null;
	
	
	private String sourceTable = null;
	
	private String targetTable = null;
	
	public Iterable(long totalSize, int pageSize, String sourceTable, String targetTable
			, String select, String updateOrInsert) {
		this.setTotalSize(totalSize);
		setPageSize(pageSize);
		setSelect(select);
		setUpdateOrInsert(updateOrInsert);
		
		setSourceTable(sourceTable);
		setTargetTable(targetTable);
	}
	
	@Override
	public java.util.Iterator<Statement> iterator() {
		return new InnerIterator();
	}

	public long getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(long totalSize) {
		this.totalSize = totalSize;
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

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	
	/**
	 * index start from 1 ~ totalSize.<br/>
	 * 
	 * @return
	 */
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
	
	public void cache(Statement stat) {
		this.cached.add(stat);
	}

	public void setContext(ExporterContext context) {
		this.context = context;
	}

	public ExporterContext getContext() {
		return context;
	}

	private class InnerIterator implements java.util.Iterator<Statement> {

		@Override
		public boolean hasNext() {
			return getIndex() <= getTotalSize();
		}

		@Override
		public Statement next() {
			Statement stat = null;
			if(cached.peek() != null) {
				stat = cached.poll();
				((StatementImpl)stat).setFrom(getIndex());
				((StatementImpl)stat).setSize(getPageSize());
			} else {
				stat = new StatementImpl(getSourceTable(), getTargetTable(), getSelect()
						, getIndex(), getPageSize(), getUpdateOrInsert());
			}
			((StatementImpl)stat).setContext(getContext());
			index += getPageSize();
			
			return stat;
		}

		@Override
		public void remove() {
			throw new RuntimeException("unsupport");
		}
		
	}

	public Deque<Statement> getCached() {
		return cached;
	}

	public void setCached(Deque<Statement> cached) {
		this.cached = cached;
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
