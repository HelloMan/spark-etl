package etl.api.dataset;

public enum  DatasetFieldName {

	RUN_EXECUTIONN_ID("RUN_KEY"),
	PIPELINE_STEP_NAME("PIPELINE_STEP_NAME"),
	ROW_KEY("ROW_KEY");

	private String name;

	public String getName() {
		return name;
	}

	DatasetFieldName(String name) {

		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}
}