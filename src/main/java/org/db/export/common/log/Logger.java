package org.db.export.common.log;

public class Logger {
	Logger() { //package access levl.
		;
	}
	
	public void debug(String message) {
		System.out.println(message);
	}
	
	public void debug(String message, Exception ex) {
		;
	}
	
	public void info(String message) {
		System.out.println(message);
	}
	
	public void info(String message, Exception ex) {
		;
	}
	
	public void error(String message) {
		System.out.println(message);
	}
	
	public void error(String message, Exception ex) {
		;
	}
	
	public void warn(String message) {
		System.out.println(message);
	}
	
	public void warn(String message, Exception ex) {
		;
	}
	
	public void fatal(String message) {
		System.out.println(message);
	}
	
	public void fatal(String message, Exception ex) {
		;
	}
}
