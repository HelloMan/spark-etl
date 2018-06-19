package etl.spark.pipeline.transform.util;

import com.google.common.collect.ImmutableMap;
import etl.spark.pipeline.transform.Jexl;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.commons.jexl3.MapContext;
import org.junit.Test;

import java.util.Map;

public class JexlTest {
	@Test
	@Ignore
	public void condition() {
		String exp = "if (x < 0) {`${y}${x} is negative`}" +
				" else " +
				"{`${y}${x} is positive`}";
		Map<String, Object> params = ImmutableMap.of("x", -1, "y", "Number: ");
		Object res = Jexl.JEXL_ENGINE.createScript(exp).execute(new MapContext(params));
		System.out.println(res);
	}
}