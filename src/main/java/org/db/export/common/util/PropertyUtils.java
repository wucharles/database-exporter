package org.db.export.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class PropertyUtils {
	private PropertyUtils()	 {
		;
	}
	
	public static Properties load(ClassLoader loader , String classpath) {
		InputStream in = loader.getResourceAsStream(classpath);
		if(in == null) {
			in = loader.getResourceAsStream("/" + classpath);
		}
		
		Properties p = new Properties();
		if( in != null) {
			try {
				p.load(in);
			} catch (IOException e) {
				; //ignore
			} finally {
				IOUtils.closeQuietly(in);
			}
		}
		
		return p;
	}
	
	public static String getTrimProperty(Properties prop
			, String key) {
		return getTrimProperty(prop, key, null);
	}
	
	public static String getTrimProperty(Properties prop
			, String key, String defaultValue) {
		if(prop == null || key == null) {
			return null;
		}
		
		String rtn = prop.getProperty(key,defaultValue);
		
		return rtn != null ? rtn.trim() : null;
		
	}
	
	public static Integer getInteger(Properties prop
			, String key, Integer defaultValue) {
		if(prop == null || key == null) {
			return null;
		}
		
		String propValue = getTrimProperty(prop, key);
		
		Integer rtn = defaultValue;
		try {
			rtn = Integer.parseInt(propValue);
		} catch (NumberFormatException e) {
			;
		}
		
		return rtn;
	}
	
	public static Boolean getBoolean(Properties prop
			, String key, Boolean defaultValue) {
		if(prop == null || key == null) {
			return null;
		}
		
		String propValue = getTrimProperty(prop, key);
		
		Boolean rtn = defaultValue;
		try {
			rtn = Boolean.parseBoolean(propValue);
		} catch (Exception e) {
			;
		}
		
		return rtn;
	}
}
