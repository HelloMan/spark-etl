package etl.spark.pipeline.transform.function;

import com.google.common.collect.ImmutableList;
import org.apache.commons.jexl3.JexlException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformFilterFunctionTest {

    private Row row;

    @Before
    public void before(){
        //arrange
        Object[] rowData = new Object[1];
        rowData[0] = 2;
        StructType structType = DataTypes.createStructType(ImmutableList.of(DataTypes.createStructField("a", DataTypes.IntegerType, true)));
        row = new GenericRowWithSchema(rowData, structType);
    }

    @Test
    public void testCall() throws Exception {
        //run
        TransformFilterFunction transformFilterFunction = new TransformFilterFunction("a>1");
        boolean result = transformFilterFunction.call(row);
        //verify
        assertThat(result).isTrue();

        //run
        transformFilterFunction = new TransformFilterFunction("a>3");
        result = transformFilterFunction.call(row);
        //verify
        assertThat(result).isFalse();
    }


    @Test(expected= JexlException.class)
    public void testCall_shouldThorwException() throws Exception {
        //run
        TransformFilterFunction transformFilterFunction = new TransformFilterFunction("a/0");
        transformFilterFunction.call(row);
    }
}