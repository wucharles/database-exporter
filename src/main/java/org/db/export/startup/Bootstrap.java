package org.db.export.startup;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * the main entry.
 * 
 * @author charles wu
 * @version 1.0.0, 2013-05-15
 *
 */
public final class Bootstrap {
	
	private static final String CONST_ROOT_PATH_PROP = "rootPath";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		parseRootPath();
		
		loadLibraries();
		
		export(args);
	}
	
	private static void export(String[] args) {
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			Class<?> clzz = loader.loadClass("org.db.export.common.runner.ExecutorService");
			Object obj = clzz.newInstance();
			
			Method method = clzz.getDeclaredMethod("run", new Class<?>[]{String[].class});
			method.invoke(obj, new Object[]{args});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * read -DrootPath=... or ../bin/bootstrap.jar
	 */
	private static void parseRootPath() {
		String rootPath = System.getProperty(CONST_ROOT_PATH_PROP);
		if(rootPath != null) {
			return;
		}
		
		Class<Bootstrap> clzz = Bootstrap.class;
		URL mainPath = clzz.getProtectionDomain().getCodeSource().getLocation();
		URL root = getRootUrl(mainPath);
		try {
			System.setProperty(CONST_ROOT_PATH_PROP, new File(root.toURI()).getAbsolutePath());
		} catch (URISyntaxException e) {
			;
		}
	}

	/**
	 * load common library & plugins.<br/>
	 * 
	 */
	private static void loadLibraries() {
		Class<Bootstrap> clzz = Bootstrap.class;
		ClassLoader parent = Thread.currentThread().getContextClassLoader();
		if(parent == null) {
			parent = clzz.getClassLoader();
		}
		if(parent == null) {
			parent = ClassLoader.getSystemClassLoader();
		}
		
		String rootPath = System.getProperty(CONST_ROOT_PATH_PROP);
		if(rootPath == null) {
			return;
		}
		System.out.println("Root Path: " + rootPath);
		URL[] urls = findLibraries(rootPath);
		URLClassLoader loader = new URLClassLoader(urls, parent);
		
		Thread.currentThread().setContextClassLoader(loader);
	}
	
	private static URL getRootUrl(URL mainPath) {
		try {
			return new URL(mainPath, "..");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	@SuppressWarnings("deprecation")
	private static URL[] findLibraries(String root) {
		final List<String> jars = new ArrayList<String>();
		jars.add(root);
		
		jars.add(root + File.separator + "lib");
		loadJars(jars, root + File.separator + "lib");
		
		jars.add(root + File.separator + "plugins");
		loadJars(jars, root + File.separator + "plugins");
		
		URL[] urls = new URL[jars.size()];
		for(int i = 0 ; i < jars.size(); ++i) {
			try {
				urls[i] = new File(jars.get(i)).toURL();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
		
		return urls;
	}
	
	private static void loadJars(List<String> jars, String path) {
		File[] files = new File(path).listFiles();
		if(files == null || files.length == 0) {
			return;
		}
		
		for(File file : files) {
			String jar = file.getAbsolutePath();
			if(jar.endsWith(".jar")) {
				jars.add(jar);
			}
		}
	}
}
