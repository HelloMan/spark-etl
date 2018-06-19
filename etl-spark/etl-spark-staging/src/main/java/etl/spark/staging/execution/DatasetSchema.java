package etl.spark.staging.execution;

import etl.api.table.Field;
import etl.spark.staging.datafields.DataRow;
import etl.spark.util.StructTypes;
import etl.api.table.Table;
import etl.spark.staging.datafields.IndexedDataField;
import etl.spark.staging.datafields.NamedDataField;
import etl.spark.staging.datafields.DataField;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.EntryStream;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class DatasetSchema implements Serializable {

    private final Table table;
    private final Map<String, Field> nameFields;
    private final Map<Integer, Field> indexedFields;
    private final StructType structType;

    public DatasetSchema(Table table) {
        this.table = table;
        this.indexedFields = buildIndexFields();
        this.nameFields = buildNameFields();
        structType = StructTypes.of(table);
    }

    private Validation<String,DataRow> validateRow(DataRow row) {
        int dataFieldCount = row.size();
        int schemaFieldCount = table.getFields().size();
        return (dataFieldCount != schemaFieldCount) ?
                Validation.invalid(String.format("Expected %d fields - actual: %d", schemaFieldCount, dataFieldCount)) : Validation.valid(row);

    }

    public Validation<String,Field> validateFieldInfo(DataField dataField) {
        if (dataField instanceof NamedDataField) {
            String fieldName = ((NamedDataField) dataField).getFieldName();
            if (nameFields.containsKey(fieldName)) {
                return Validation.valid(nameFields.get(fieldName));
            }
            return Validation.invalid(String.format("The field with name %s is undefined, please check schema.", fieldName));
        } else {
            int index = ((IndexedDataField) dataField).getIndex();
            if (indexedFields.containsKey(index)) {
                return Validation.valid(indexedFields.get(index));
            }
            return Validation.invalid(String.format("The field with index %d is undefined, please check schema.", index));
        }
    }

    private Optional<Field> findFieldInfo(DataField dataField) {
        if (dataField instanceof NamedDataField) {
            String fieldName = ((NamedDataField) dataField).getFieldName();
            return Optional.ofNullable(nameFields.get(fieldName));
        } else {
            int index = ((IndexedDataField) dataField).getIndex();
            return Optional.ofNullable(indexedFields.get(index));
        }
    }



    private Map<Integer, Field> buildIndexFields() {
        return EntryStream.of(table.getFields()).toMap();

    }

    private Map<String, Field> buildNameFields() {
        return table.getFields()
                .stream()
                .collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    private Validation<String,DataRow> validateFields(DataRow row) {
        List<String> invalidations = row.getFields().stream()
                .map(this::validateField)
                .filter(Validation::isInvalid)
                .map(Validation::getError)
                .collect(Collectors.toList());
        if (invalidations!= null && !invalidations.isEmpty()) {
            return Validation.invalid(String.join(";", invalidations));
        }
        return Validation.valid(row);
    }

    private Validation<String,DataField>   validateField(DataField dataField ) {
        Validation<String, Field> fieldSchemaValidation = validateFieldInfo(dataField);
        if (fieldSchemaValidation.isValid()) {
            Validation<List<String>,String> fieldValidations = fieldSchemaValidation.get().validate(dataField.getValue());
            if (fieldValidations.isInvalid()) {
                return Validation.invalid(String.join(";", fieldValidations.getError()));
            }
        }else{
            return Validation.invalid(fieldSchemaValidation.getError());
        }

        return Validation.valid(dataField);

    }


    public Validation<String,DataRow> validate(DataRow dataRow) {
        Validation<String, DataRow> rowValidation = validateRow(dataRow);
        if (rowValidation.isValid()) {
            return validateFields(dataRow);
        } else {
            return rowValidation;
        }
    }

    public Object parseField(DataField dataField) {
        Field field = findFieldInfo(dataField).get();
        return field.parse(dataField.getValue());

    }


}