package org.db.export.common.runner.type;

public enum ExportMode {
	FULL("FULL")/*全量*/, INC("INC")/*增量*/;

	private String name = null;

	ExportMode(String name) {
		setName(name);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static ExportMode getByName(String name) {
		ExportMode[] values = ExportMode.values();
		for(ExportMode value : values) {
			if(value.getName().equalsIgnoreCase(name)) {
				return value;
			}
		}

		return ExportMode.FULL;
	}
}
