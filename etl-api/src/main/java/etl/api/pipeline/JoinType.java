package etl.api.pipeline;

public enum JoinType {
	LEFT("left_outer"), RIGHT("right_outer"), INNER("inner");

	private String joinType;

	JoinType(String joinType) {
		this.joinType = joinType;
	}

	@Override
	public String toString() {
		return joinType;
	}
}