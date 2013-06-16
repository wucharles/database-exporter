package org.db.export.common.runner;

import org.db.export.common.ExporterContext;
import org.db.export.common.RunnerException;

public interface Runner {
	void execute(ExporterContext context) throws RunnerException;
}
