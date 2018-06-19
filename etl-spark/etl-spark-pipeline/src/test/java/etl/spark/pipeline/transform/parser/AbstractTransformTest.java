package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.SourceLoader;
import etl.spark.core.DriverContext;
import etl.spark.pipeline.core.AbstractJobOperator;
import etl.spark.pipeline.transform.TransformUtil;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractTransformTest {

    @Autowired
    @Getter
    private AbstractJobOperator jobOperator;

    @Autowired
	protected DriverContext driverContext;



    static SourceLoader loadEmployee() {
        return SourceLoader.builder().name("emp")
                .source(TransformUtil.getCsvSource("data/emp.csv"))
                .build();
    }


}
