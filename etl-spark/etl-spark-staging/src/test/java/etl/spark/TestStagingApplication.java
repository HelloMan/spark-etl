package etl.spark;

import etl.common.annotation.ExcludeFromTest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(lazyInit = true,excludeFilters = @ComponentScan.Filter(type = FilterType.ANNOTATION, value = ExcludeFromTest.class))
public class TestStagingApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TestStagingApplication.class).web(false).run(args);
    }


}
