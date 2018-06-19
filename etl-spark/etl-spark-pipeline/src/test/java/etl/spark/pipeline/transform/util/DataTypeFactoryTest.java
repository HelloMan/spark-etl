package etl.spark.pipeline.transform.util;

import etl.api.pipeline.FieldType;
import etl.spark.pipeline.transform.DataTypeFactory;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class DataTypeFactoryTest {


    @Test
    public void testGetSparkDataType_ForInt() throws Exception {
        Assertions.assertThat(DataTypeFactory.getDataType(FieldType.Type.INT)).isEqualTo(DataTypes.IntegerType);
    }
    @Test
    public void testGetSparkDataType_ForBoolean() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.BOOLEAN)).isEqualTo(DataTypes.BooleanType);
    }
    @Test
    public void testGetSparkDataType_ForLong() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.LONG)).isEqualTo(DataTypes.LongType);
    }
    @Test
    public void testGetSparkDataType_ForDouble() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.DOUBLE)).isEqualTo(DataTypes.DoubleType);
    }
    @Test
    public void testGetSparkDataType_ForFloat() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.FLOAT)).isEqualTo(DataTypes.FloatType);
    }
    @Test
    public void testGetSparkDataType_ForString() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.STRING)).isEqualTo(DataTypes.StringType);
    }

    @Test
    public void testGetSparkDataType_ForDate() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.DATE)).isEqualTo(DataTypes.DateType);
    }
    @Test
    public void testGetSparkDataType_ForTimeStamp() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.TIMESTAMP)).isEqualTo(DataTypes.TimestampType);
    }
    @Test
    public void testGetSparkDataType_ForNull() throws Exception {
        assertThat(DataTypeFactory.getDataType(FieldType.Type.NULL)).isEqualTo(DataTypes.NullType);
    }


}