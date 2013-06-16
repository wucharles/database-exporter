package org.db.export;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

public class SimpleTest {
	public void testTrue() {
		;
	}
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		URL root = SimpleTest.class.getClassLoader().getResource("config/mapping.properties");
		
		Properties p = new Properties();
		p.load(root.openStream());
		
		System.out.println(p.getProperty("test"));
	}
}
