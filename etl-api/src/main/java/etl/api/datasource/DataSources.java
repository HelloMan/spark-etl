package etl.api.datasource;

import lombok.experimental.UtilityClass;

@UtilityClass
public class DataSources {

    public static CsvDataSource csv(String filePath) {
        return CsvDataSource.builder().filePath(filePath).build();
    }
}
