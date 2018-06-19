package etl.spark.core.phoenix;

import com.google.common.collect.ImmutableList;
import etl.common.jdbc.JdbcUtils;
import etl.spark.core.DataBaseSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class PhoenixSchema implements DataBaseSchema {

    public static final Metadata ROW_KEY_METADATA = new MetadataBuilder().putBoolean("rowKey", true).build();

    private static final String CREATE_PATTERN = "CREATE TABLE %s (%s) SALT_BUCKETS=%d";

    private static final Map<String, Function<DataType, String>> dataTypeToFieldConverters = new HashMap<>();

    private final String zookeeperUrl;
    private final String hbaseRootDir;

    private final int saltBuckets;

    static {
        dataTypeToFieldConverters.put(DataTypes.IntegerType.simpleString(), dataType -> "INTEGER");
        dataTypeToFieldConverters.put(DataTypes.LongType.simpleString(), dataType -> "BIGINT");
        dataTypeToFieldConverters.put(DataTypes.BooleanType.simpleString(), dataType -> "BOOLEAN");
        dataTypeToFieldConverters.put(DecimalType$.MODULE$.simpleString(), dataType -> {
            DecimalType decimalType = (DecimalType) dataType;
            return String.format("DECIMAL(%d,%d)", decimalType.scale(), decimalType.precision());
        });
        dataTypeToFieldConverters.put(DataTypes.DoubleType.simpleString(), dataType -> "DOUBLE");
        dataTypeToFieldConverters.put(DataTypes.StringType.simpleString(), dataType -> "VARCHAR");
        dataTypeToFieldConverters.put(DataTypes.DateType.simpleString(), dataType -> "DATE");
        dataTypeToFieldConverters.put(DataTypes.TimestampType.simpleString(), dataType -> "TIMESTAMP");
    }

    public PhoenixSchema(String zookeeperUrl, String hbaseRootDir, int saltBuckets) {
        this.zookeeperUrl = zookeeperUrl;
        this.hbaseRootDir = hbaseRootDir;
        this.saltBuckets = saltBuckets;
    }

    public boolean tableExists(String tableName) throws SQLException {
        try (Connection connection = getConnection()) {
            return JdbcUtils.tableExists(connection, tableName);
        }
    }

    @Override
    public Optional<JdbcType> getJdbcType(DataType dataType) {
        if (DataTypes.IntegerType == dataType) {
            return Optional.of(new JdbcType("INTEGER", Types.INTEGER));
        } else  {
            return Optional.of(new JdbcType("BIGINT", Types.BIGINT));
        }
    }


    @Override
    public void createTable(String tableName, StructType structType) throws SQLException {
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                String sql = getCreateDdl(structType, tableName);
                log.info("create phoenix table with sql: {}", sql);
                statement.executeUpdate(sql);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        String connStr = String.format("jdbc:phoenix:%s:%s", zookeeperUrl, hbaseRootDir);
        log.info("opening phoenix jdbc connection with: {}", connStr);
        return DriverManager.getConnection(connStr);
    }


    private String getCreateDdl(StructType structType, String tableName) {
        String fieldsDDL = Arrays.stream(structType.fields()).map(this::getFieldDDL).collect(Collectors.joining(", "));
        String sql = String.format(CREATE_PATTERN,
                tableName.toUpperCase(),
                fieldsDDL,
                saltBuckets);
        return sql;
    }

    private String getFieldDDL(StructField field) {
        ImmutableList.Builder<String> fieldBuilder = ImmutableList.builder();
        fieldBuilder.add(field.name().toUpperCase());
        getJdbcType(field.dataType()).ifPresent(jdbcType -> fieldBuilder.add(jdbcType.databaseTypeDefinition()));
        fieldBuilder.add(getJdbcType(field.dataType()).get().databaseTypeDefinition());
        if (PhoenixSchema.ROW_KEY_METADATA.equals(field.metadata())) {
            fieldBuilder.add("PRIMARY KEY");
        }
        if (!field.nullable()) {
            fieldBuilder.add("NOT NULL");
        }
        return String.join(" ", fieldBuilder.build());
    }


}