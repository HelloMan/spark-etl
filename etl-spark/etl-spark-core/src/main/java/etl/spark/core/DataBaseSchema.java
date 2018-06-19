package etl.spark.core;

import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Optional;

public interface DataBaseSchema {

    boolean tableExists(String tableName) throws SQLException;

    Optional<JdbcType> getJdbcType(DataType dataType);

    void createTable(String tableName, StructType structType) throws SQLException;
}
