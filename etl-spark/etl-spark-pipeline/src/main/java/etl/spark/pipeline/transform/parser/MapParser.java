package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Map;
import etl.api.pipeline.MapField;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import etl.spark.pipeline.transform.DataTypeFactory;
import etl.spark.pipeline.transform.Jexl;
import etl.spark.util.DatasetRow;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.MapContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class MapParser extends AbstractTransformParser<Map> {


    public MapParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        super(transformDatasetService,stepExecution);
    }

    @AllArgsConstructor
    private static class FieldResolver implements Serializable {

        private final MapField field;

        public Object resolve(Row r) {
            Object result = evaluate(field.getName(), new DatasetRow(r).asMap());
            return field.getFieldType().parse(result);
        }

        private Object evaluate(String expression, java.util.Map<String, Object> rowMap) {
            JexlContext jexlContext = new MapContext(rowMap);
            return Jexl.JEXL_ENGINE.createScript(expression).execute(jexlContext);
        }
    }


    @AllArgsConstructor
    @NoArgsConstructor
    private static class RowMapper implements Function<Row, Row> {

        private Map transform;


        @Override
        public Row call(Row row) throws Exception {
            List<Object> fieldValues = new ArrayList<>(row.size() + transform.getFields().size());
            fieldValues.addAll(JavaConversions.seqAsJavaList(row.toSeq()));
            for (MapField field : transform.getFields()) {
                fieldValues.add(new FieldResolver(field).resolve(row));
            }
            return RowFactory.create(fieldValues.toArray());
        }


    }


    @Override
    public void parse(final Map transform) {
        Dataset<Row> inputDataSet = transformDatasetService.getDataset(stepExecution,transform.getInput())
                .orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", transform.getInput())));

        // because our domain model doesn't treat filter as mapTry, we need to use Map mapTry to do filter mapTry,
        // if no map fields assigned, then just do filter.
        if (CollectionUtils.isNotEmpty(transform.getFields())) {
            final JavaRDD<Row> javaRdd = inputDataSet.toJavaRDD().map(new RowMapper(transform));
            inputDataSet = inputDataSet.sparkSession().createDataFrame(javaRdd, createStructType(transform, inputDataSet));

        }
        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform), inputDataSet);
    }

    private StructType createStructType(Map transform, Dataset<Row> inputDataSet) {
        final List<StructField> structFields = new ArrayList<>();
        structFields.addAll(Arrays.asList(inputDataSet.schema().fields()));

        for (MapField mapField : transform.getFields()) {
            StructField structField = DataTypes.createStructField(mapField.getAlias(),
                    DataTypeFactory.getDataType(mapField.getFieldType().getType()), true);
            structFields.add(structField);
        }

        return DataTypes.createStructType(structFields);

    }


}