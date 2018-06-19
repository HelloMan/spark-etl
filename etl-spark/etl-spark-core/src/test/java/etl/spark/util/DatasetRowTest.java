package etl.spark.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
public class DatasetRowTest {

    @Test
    public void testAsMap() throws Exception {
        StructField structField1 = new StructField("id1", DataTypes.IntegerType, true, Metadata.empty());
        StructField structField2 = new StructField("id2", DataTypes.IntegerType, true, Metadata.empty());
        StructField structField3 = new StructField("RUN_KEY", DataTypes.LongType, true, Metadata.empty());
        StructField[] fields = {structField1, structField2,structField3};
        StructType structType = new StructType(fields);
        Object[] rows = new Object[]{1, 2,1};
        Row row = new GenericRowWithSchema(rows,structType);
        Map<String, Object> result = new DatasetRow(row).asMap();
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get("id1")).isEqualTo(1);
    }
}