package org.db.export.common;

import java.sql.Connection;

public interface Statement {
	void bind(Connection src, Connection target);
	
	void exec() throws RunnerException;
	
	boolean isBinded();
	
	void unbind();
}