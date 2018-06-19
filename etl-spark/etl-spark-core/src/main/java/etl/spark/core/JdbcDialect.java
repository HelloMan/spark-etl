package etl.spark.core;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

/**
 * Created by chaojun on 17/11/27.
 */
public abstract class JdbcDialect extends org.apache.spark.sql.jdbc.JdbcDialect {

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        return super.getCatalystType(sqlType, typeName, size, md);
    }
}
