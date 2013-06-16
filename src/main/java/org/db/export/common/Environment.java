package org.db.export.common;

import java.io.File;


public final class Environment {
	
	private static String CONST_ROOT_PATH = "rootPath";
	
	private static String CONST_LIB_PATH = "lib";
	
	private static String CONST_PLUGIN_PATH = "plugins";
	
	/**
	 * the path of ../bin/bootstrap.jar
	 */
	private static String rootPath = null;
	
	private static String libPath = null;
	
	private static String pluginPath = null;
	
	private Environment() {
		;
	}
	
	public static String getRootPath() {
		return rootPath;
	}
	
	public static String getLibPath() {
		return libPath;
	}
	
	public static String getPluginPath() {
		return pluginPath;
	}
	
	static {
		rootPath = System.getProperty(CONST_ROOT_PATH);
		
		libPath = rootPath + File.separator + CONST_LIB_PATH;
		
		pluginPath = rootPath + File.separator + CONST_PLUGIN_PATH;
	}
}
