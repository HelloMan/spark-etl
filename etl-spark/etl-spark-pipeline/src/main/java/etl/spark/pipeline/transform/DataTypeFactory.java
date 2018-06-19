package etl.spark.pipeline.transform;

import etl.api.pipeline.FieldType;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.DataType;

@UtilityClass
public class DataTypeFactory {

    public static DataType getDataType(FieldType.Type dataType) {
        switch (dataType) {
            case INT:
                return org.apache.spark.sql.types.DataTypes.IntegerType;
            case LONG:
                return org.apache.spark.sql.types.DataTypes.LongType;
            case FLOAT:
                return org.apache.spark.sql.types.DataTypes.FloatType;
            case DOUBLE:
                return org.apache.spark.sql.types.DataTypes.DoubleType;
            case DATE:
                return org.apache.spark.sql.types.DataTypes.DateType;
            case TIMESTAMP:
                return org.apache.spark.sql.types.DataTypes.TimestampType;
            case BOOLEAN:
                return org.apache.spark.sql.types.DataTypes.BooleanType;
            case NULL:
                return org.apache.spark.sql.types.DataTypes.NullType;
            default:
                return org.apache.spark.sql.types.DataTypes.StringType;
        }
    }
}
