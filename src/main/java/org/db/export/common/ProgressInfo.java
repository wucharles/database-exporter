package org.db.export.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.db.export.common.log.Logger;
import org.db.export.common.log.LoggerFactory;
import org.db.export.common.type.Triple;
import org.db.export.common.util.CommonUtils;

public class ProgressInfo {
	private static final Logger log = LoggerFactory.getLogger(ProgressInfo.class);

	private static final String CONST_PER_FORMAT = "%.0f";

	private List<String> exportTables = null;

	private List<List<String>> cells = null;

	private Map<String, Triple<Long/*total rows*/, AtomicLong /*exported rows*/,AtomicLong /*printed rows*/>> tableMapping = null;

	public void init(Set<String> tables) {
		exportTables = new ArrayList<String>(tables);
		cells = CommonUtils.slice(exportTables, 5);

		Collections.sort(exportTables);

		tableMapping = new ConcurrentHashMap<String, Triple<Long, AtomicLong, AtomicLong>>(exportTables.size(), 1F);
		for(String table : exportTables) {
			Triple<Long, AtomicLong, AtomicLong> triple = new Triple<Long, AtomicLong, AtomicLong>();
			triple.value1 = -1L;
			triple.value2 = new AtomicLong(0);
			triple.value3 = new AtomicLong(0);

			tableMapping.put(table, triple);
		}
	}

	public void setTotalRows(String table, Long rows) {
		if(!tableMapping.containsKey(table)) {
			log.warn("No table in Executor Context: " + table);

			return;
		}

		tableMapping.get(table).value1 = rows;
	}

	public void inc(String table, Long rows) {
		if(!tableMapping.containsKey(table)) {
			log.warn("No table in Executor Context: " + table);

			return;
		}

		final Triple<Long, AtomicLong, AtomicLong> triple = tableMapping.get(table);

		long total = triple.value1;
		if(total < 0) {
			log.warn("Table[" + table + "]tatoal row size is set incorrectly, please check.<br/>");

			return;
		}

		long oldRows = triple.value3.get();
		long newRows = triple.value2.addAndGet(rows);

		//每10%进度打印一次，或者已经100%
		if( total > 0 && (((double)(newRows - oldRows))/total * 100 >= 10
				|| newRows / total == 1)) {
			triple.value3.set(triple.value2.get());

			print();
		}
	}

	private void print() {
		StringBuilder sb = new StringBuilder("Executor progress\n");

		sb.append("\t");
		for(List<String> cell : cells) {
			for (String table : cell) {
				Triple<Long, AtomicLong, AtomicLong> tuple = tableMapping.get(table);
				double per = 0;
				if(tuple.value1 > 0) { //total number has been seted or is not null;
					per = (double) (tuple.value2.get()) / tuple.value1 * 100;
				} else if(tuple.value1 == 0) {
					per = 100;
				}

				sb.append(table).append("：").append(String.format(CONST_PER_FORMAT, per)).append("%, ");
			}
			sb.setLength(sb.length() - 2);
			sb.append("\n\t");
		}
		sb.setLength(sb.length() - 1);

		log.info(sb.toString());
	}
}
