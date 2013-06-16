package org.db.export.common.type;

import java.sql.Types;

public class ColumnMetaData {
	private String name;
	
	private int type = Types.INTEGER;
	
	private boolean isNullable = false;
	
	private String defaultValue = null;
	
	//the length of type, is used in string or decimal type.
	private int size = -1; 
	
	//the number of digit char('.');
	private int digits = -1; //小数点位数
	
	private  MetaDataStatus status = MetaDataStatus.NORMAL;
	
	private TableMetaData tableMetaData = null;
	
	public ColumnMetaData(TableMetaData tableMetaData) {
		setTableMetaData(tableMetaData);
	}
	
	public ColumnMetaData copy(TableMetaData tableMetaData) {
		ColumnMetaData md = new ColumnMetaData(tableMetaData);
		md.name = this.getName();
		md.type = this.getType();
		md.isNullable = this.isNullable();
		md.defaultValue = this.getDefaultValue();
		md.size = this.getSize();
		md.digits = this.getDigits();
		md.status = this.getStatus();
		
		return md;
	}

	public String getName() {
		return name;
	}
	
	public String getSourceName() {
		String sourceName = tableMetaData.getColumnNameMapping().get(getName());
		return sourceName == null ? getName() : sourceName;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getDigits() {
		return digits;
	}

	public void setDigits(int digits) {
		this.digits = digits;
	}

	public MetaDataStatus getStatus() {
		return status;
	}

	public void setStatus(MetaDataStatus status) {
		this.status = status;
	}

	public TableMetaData getTableMetaData() {
		return tableMetaData;
	}

	public void setTableMetaData(TableMetaData tableMetaData) {
		this.tableMetaData = tableMetaData;
	}
}
