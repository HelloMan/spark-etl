package etl.spark.core.phoenix;

import com.google.common.base.Preconditions;
import javaslang.Tuple2;
import lombok.NonNull;

class PhoenixTableRowKey {
	private static final Long UNSIGNED_MAX_INT = 0xFFFFFFFFL;

	private Integer runKey;

	private PhoenixTableRowKey(Integer runKey) {
		this.runKey = runKey;
	}

	private static boolean lessThanMaxUnsignedInt(Long longValue) {
		return Long.compareUnsigned(longValue, UNSIGNED_MAX_INT) < 0;
	}

	/**
	 * Currently assume that unsigned int is big enough for run key and row sequence.
	 */
	public static PhoenixTableRowKey of(@NonNull Long runKey) {
		Preconditions.checkArgument(lessThanMaxUnsignedInt(runKey), "Run key exceeds range of unsigned integer");
		return new PhoenixTableRowKey(runKey.intValue());
	}

	public Long buildRowKey(@NonNull Long rowSeq) {
		Preconditions.checkArgument(lessThanMaxUnsignedInt(rowSeq), "Row sequence of dataset exceeds range of unsigned integer");
		return rowKeyHead() | rowSeq;
	}

	public Tuple2<Long, Long> getRowKeyRange() {
		Long start = rowKeyHead();
		Long end = start | UNSIGNED_MAX_INT;
		return new Tuple2(start, end);
	}

	private Long rowKeyHead() {
		return runKey.longValue() << 32;
	}
}