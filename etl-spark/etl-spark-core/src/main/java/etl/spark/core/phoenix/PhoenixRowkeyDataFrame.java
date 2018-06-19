package etl.spark.core.phoenix;

import etl.api.dataset.DatasetFieldName;
import one.util.streamex.StreamEx;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;

public class PhoenixRowkeyDataFrame {
    private final SparkSession spark;
    private final Dataset<Row> dataFrame;

    PhoenixRowkeyDataFrame(SparkSession spark, Dataset<Row> dataFrame) {
        this.spark = spark;
        this.dataFrame = dataFrame;
    }

    public Dataset<Row> withRowkey(Long runKey) {
        StructType structType = schemaWithRowkey();
        JavaRDD<Row> withRowkeyRdd = dataWithRowkey(runKey);
        return spark.createDataFrame(withRowkeyRdd, structType);
    }

    private JavaRDD<Row> dataWithRowkey(Long runKey) {
        // our row key field always locates in the beginning
        boolean hasRowKeyField = Arrays.stream(dataFrame.schema().fieldNames()).anyMatch(DatasetFieldName.ROW_KEY.getName()::endsWith);
        JavaPairRDD<Row, Long> rdd = dataFrame.javaRDD().zipWithUniqueId();
        return rdd.map(tuple -> {
            List<Object> row = JavaConversions.seqAsJavaList(tuple._1().toSeq());
            Long rowKey = PhoenixTableRowKey.of(runKey).buildRowKey(tuple._2());
            StreamEx<Object> rowKeyRow = StreamEx.of((Object) rowKey);
            // if dataset includes ROW_KEY field already, need to update to new row key value by current run key
            StreamEx<Object> wholeRow = hasRowKeyField ?
                    rowKeyRow.append(javaslang.collection.List.ofAll(row).tail().toJavaList()) :
                    rowKeyRow.append(row);
            return RowFactory.create(wholeRow.toArray());
        });
    }

    private StructType schemaWithRowkey() {
        StructType oldSchema = dataFrame.schema();
        boolean rowKeyFieldExists = StreamEx.of(Arrays.asList(oldSchema.fieldNames()))
                .anyMatch(f -> f.equals(DatasetFieldName.ROW_KEY.getName()));
        if (rowKeyFieldExists) {
            return oldSchema;
        }

        StructField rowKeyField = new StructField(DatasetFieldName.ROW_KEY.getName(), DataTypes.LongType, false, PhoenixSchema.ROW_KEY_METADATA);
        StreamEx<StructField> newFields = StreamEx.of(rowKeyField).append(oldSchema.fields());
        return new StructType(newFields.toArray(StructField.class));
    }
}