package etl.spark.core.phoenix;

import javaslang.Tuple2;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PhoenixTableRowKeyTest {

	private PhoenixTableRowKey helper;

	@Before
	public void init() {
		Long runKey = Integer.valueOf(1).longValue();
		helper = PhoenixTableRowKey.of(runKey);
	}

	@Test
	public void testBuildRowKey() throws Exception {
		Long rowKey = helper.buildRowKey(1L);
		assertThat(rowKey).isEqualTo(0x0000000100000001L);
	}

	@Test
	public void testGetRowKeyRange() throws Exception {
		Tuple2<Long, Long> range = helper.getRowKeyRange();
		assertThat(range._1).isEqualTo(0x0000000100000000L);
		assertThat(range._2).isEqualTo(0x00000001FFFFFFFFL);
	}

}