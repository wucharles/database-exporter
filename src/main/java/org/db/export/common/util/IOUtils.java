package org.db.export.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;

public final class IOUtils {
	private IOUtils() {
		;
	}
	
	public static String readLine(URL url) {
		InputStream in = null;
		LineNumberReader reader = null;
		try {
			in = url.openStream();
			
			reader = new LineNumberReader(new InputStreamReader(in, "UTF-8"));
			return reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeQuietly(in);
			closeQuietly(reader);
		}
		
		return null;
	}
	
	public static void closeQuietly(InputStream in) {
		if(in != null) {
			try {
				in.close();
			} catch (IOException e) {
				; //ingnore;
			}
		}
	}
	
	public static void closeQuietly(Reader reader) {
		if(reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				; //ingnore;
			}
		}
	}
	
	public static void closeQuietly(Writer writer) {
		if(writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				; //ingnore;
			}
		}
	}
}
