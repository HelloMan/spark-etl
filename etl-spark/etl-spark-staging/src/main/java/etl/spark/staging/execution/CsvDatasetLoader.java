package etl.spark.staging.execution;

import etl.api.dataset.DatasetPath;
import etl.api.datasource.CsvDataSource;
import etl.api.job.staging.StagingItemRequest;
import etl.common.csv.CsvConf;
import etl.common.csv.CsvLineParser;
import etl.spark.core.DriverContext;
import etl.spark.staging.config.StagingConfig;
import etl.spark.staging.datafields.DataRow;
import etl.spark.staging.datafields.IndexedDataField;
import etl.spark.staging.exception.DatasetLoaderException;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.EntryStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@Slf4j
@Component
public class CsvDatasetLoader implements DatasetLoader ,Serializable {

    @Autowired
    private transient JavaSparkContext javaSparkContext;

    @Autowired
    private transient DriverContext driverContext;

    @Autowired
    private transient StagingConfig stagingConfig;


    @Override
    public JavaRDD<DataRow> load(StagingItemRequest item) {
        CsvDataSource src = (CsvDataSource) item.getSource();
        CsvConf conf = getCsvConf(src);
        DatasetPath datasetPath = new DatasetPath(driverContext.getJobExecutionId(),
                item.getDataset().getName(),
                stagingConfig.getDatasetRootPath());
        log.debug("Loading HDFS file as RDD: {}", datasetPath.getStagingFile());
        JavaRDD<String> rdd = loadCsvSource(src, datasetPath.getStagingFile());
        JavaRDD<DataRow> srcFieldsRDD = rdd.map(r -> extractFields(r, conf));
        return srcFieldsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }

    private JavaRDD<String> loadCsvSource(CsvDataSource src, String filePath) {
        final JavaRDD<String> rdd = javaSparkContext.textFile(filePath);
        final int start = src.isHeader() ? src.getStart() + 1 : src.getStart();
        final int end = src.getEnd();
        if (start > 0 || end > 0) {
            return excludeRows(rdd, start, end);
        }
        return rdd;
    }

    private  <T> JavaRDD<T> excludeRows(JavaRDD<T> rdd, int startIndex, int endIndex) {
        Function<Tuple2<T, Long>, Boolean> checkIndex = t -> {
            Long index = t._2();
            boolean afterStart = true;
            if (startIndex > 0 && index < startIndex) {
                afterStart = false;
            }
            if (endIndex > 0 && index > endIndex) {
                afterStart = false;
            }
            return afterStart;
        };

        return rdd.zipWithIndex()
                .filter(checkIndex)
                .map(Tuple2::_1);
    }


    private DataRow extractFields(String row, CsvConf conf) {
        try {
            List<IndexedDataField> dataFields = EntryStream.of(new CsvLineParser(row, conf).parse(true))
                    .map(entry -> new IndexedDataField(entry.getKey(), entry.getValue()))
                    .toList();
            return new DataRow(dataFields);
        } catch (IOException e) {
            throw new DatasetLoaderException(e);
        }
    }

    private CsvConf getCsvConf(CsvDataSource csvSource) {
        char delimiter = csvSource.getDelimiter();
        String quote = csvSource.getQuote();
        CsvConf.CsvConfBuilder csvConfBuilder = CsvConf.builder();
        if (delimiter != 0) {
            csvConfBuilder.delimiter(delimiter);
        }
        if (StringUtils.isNoneBlank(quote)) {
            csvConfBuilder.quote(quote);
        }
        return csvConfBuilder.build();
    }

}