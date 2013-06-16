package org.db.export.common.type;

public enum MetaDataStatus {
	NORMAL,			
	ONLY_IN_SRC,		//Only exists in source database, column or table.
	ONLY_IN_DEST;   //Only exists in destination database, column or table.
}
