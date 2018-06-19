package etl.spark.util;


import etl.api.table.Table;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@UtilityClass
public class StructTypes {

    public static StructType of(Table table) {
        StructField[] structFields = table.getFields()
                .stream()
                .map(FieldConverter::convertToStructField)
                .toArray(StructField[]::new);
        return new StructType(structFields);
    }


}
