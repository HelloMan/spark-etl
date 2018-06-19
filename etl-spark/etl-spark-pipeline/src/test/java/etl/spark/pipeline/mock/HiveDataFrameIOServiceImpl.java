package etl.spark.pipeline.mock;

import etl.spark.core.hive.HiveDataFrameIOService;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

@Service
@Primary
public class HiveDataFrameIOServiceImpl extends HiveDataFrameIOService {
}