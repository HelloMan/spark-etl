package etl.spark.staging.utils;

import etl.api.table.Table;
import etl.common.json.MapperWrapper;

import java.io.IOException;

public class TableFactory {

    public static Table createEmployeeSchema() throws IOException {
        return MapperWrapper.MAPPER.readValue(TableFactory.class.getClassLoader().getResourceAsStream("datasets/schema/employee.json"),
                Table.class);

    }
}
