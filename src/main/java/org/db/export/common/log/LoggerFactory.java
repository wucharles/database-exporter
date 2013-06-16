package org.db.export.common.log;

public final class LoggerFactory {
	private LoggerFactory() {
		;
	}
	
	public static Logger getLogger(Class<?> clzz) {
		return new Logger();
	}
}
