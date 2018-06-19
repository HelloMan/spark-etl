package etl.common.jdbc;

import lombok.Builder;
import lombok.NonNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

@Builder
public class JdbcUtils {

	public static boolean tableExists(Connection connection, @NonNull String tableName) throws SQLException {
		try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
			return rs.next();
		}
	}
}