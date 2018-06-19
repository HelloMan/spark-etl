package etl.spark.pipeline.transform.function;

import etl.spark.pipeline.transform.Jexl;
import etl.spark.util.DatasetRow;
import lombok.AllArgsConstructor;
import org.apache.commons.jexl3.MapContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
@SuppressWarnings("squid:UndocumentedApi")
@AllArgsConstructor
public class TransformFilterFunction implements FilterFunction<Row>,Serializable {

    private String filter;
    @Override
    public boolean call(Row v1) throws Exception {
        Object result =  Jexl.JEXL_ENGINE.createExpression(filter).evaluate(new MapContext(new DatasetRow(v1).asMap()));
        if (result == null) {
			return false;
		}
		if (Boolean.class.isAssignableFrom(result.getClass())) {
            return (boolean) result;
        }
        return Boolean.valueOf(result.toString());
    }
}
