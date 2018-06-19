package etl.spark.util;

import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

import java.util.Arrays;

public final class DatasetRow {

    private final Row row;

    public DatasetRow(Row row) {
        this.row = row;
    }

    /**
     * convert row instance to {@link java.util.Map}
     * @return a Java map
     */
    public java.util.Map<String, Object> asMap() {
        Iterator<String> fieldsItr = JavaConversions.asScalaIterator(Arrays.asList(row.schema().fieldNames()).iterator());
        return JavaConversions.mapAsJavaMap(row.getValuesMap(fieldsItr.toSeq()));
    }
}
