package etl.spark.util;


import com.google.common.collect.ImmutableMap;
import etl.api.table.BooleanField;
import etl.api.table.DateField;
import etl.api.table.DecimalField;
import etl.api.table.DoubleField;
import etl.api.table.Field;
import etl.api.table.IntField;
import etl.api.table.LongField;
import etl.api.table.StringField;
import etl.api.table.TimestampField;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public interface FieldConverter<F,T> {

    T convert(F object);

    static StructField convertToStructField(Field field) {
        return StructFieldConverter.DEFAULT.convert(field);
    }

    final class StructFieldConverter implements FieldConverter<Field,StructField> {

        public static final StructFieldConverter DEFAULT = new StructFieldConverter();

        private final ImmutableMap<Class<? extends Field>, FieldConverter<Field,StructField>> fieldToStructFields;

        private StructFieldConverter() {

            ImmutableMap.Builder<Class<? extends Field>, FieldConverter<Field, StructField>> builder = ImmutableMap.builder();

            builder.put(BooleanField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.BooleanType, field.isNullable(), Metadata.empty()));
            builder.put(IntField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.IntegerType, field.isNullable(), Metadata.empty()));
            builder.put(LongField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.LongType, field.isNullable(), Metadata.empty()));
            builder.put(DoubleField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.DoubleType, field.isNullable(), Metadata.empty()));
            builder.put(StringField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.StringType, field.isNullable(), Metadata.empty()));
            builder.put(DateField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.DateType, field.isNullable(), Metadata.empty()));
            builder.put(TimestampField.class, field -> new StructField(field.getName().toUpperCase(), DataTypes.TimestampType, field.isNullable(), Metadata.empty()));

            builder.put(DecimalField.class, field -> {
                DecimalField decimalField = (DecimalField) field;
                DataType dataType = DataTypes.createDecimalType(decimalField.getPrecision(), decimalField.getScale());
                return new StructField(field.getName().toUpperCase(), dataType, field.isNullable(), Metadata.empty());
            });
            this.fieldToStructFields = builder.build();
        }

        @Override
        public StructField convert(Field object) {
            return fieldToStructFields.get(object.getClass()).convert(object);
        }
    }
}

