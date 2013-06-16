package org.db.export.common;

public class RunnerException extends Exception{
	private static final long serialVersionUID = 3089935320977354049L;

	public RunnerException() {
		;
	}
	
	public RunnerException(String message) {
		super(message);
	}
	
	public RunnerException(String message, Throwable ex) {
		super(message, ex);
	}
}
