package org.db.export.common.runner;

import org.db.export.common.RunnerException;
import org.db.export.common.runner.config.Configuration;
import org.db.export.common.runner.impl.ParellelRunner;

public class ExecutorService {
	public void run(String[] args) {
		Configuration config = buildConfig(args);
		
		export(config);
	}
	
	private static void export(Configuration config) {
		String runner = System.getProperty("runner");

		Runner runnerInstance = null;
		try {
			if (runner != null) {
				Class<?> clzz = Class.forName(runner);
				if (Runner.class.isAssignableFrom(clzz)) {
					runnerInstance = (Runner) (clzz.newInstance());
				}
			}
		} catch (ClassNotFoundException e) {
			;
		} catch (InstantiationException e) {
			;
		} catch (IllegalAccessException e) {
			;
		}

		if(runnerInstance == null) {
			runnerInstance = new ParellelRunner();
		}

		try {
			runnerInstance.execute(config.getContext());
		} catch (RunnerException e) {
			;
		}
	}
	
	private static Configuration buildConfig(String[] args) {
		Configuration config = new Configuration();
		config.setCommandLine(args);
		
		config.config();
		
		return config;
	}
}
