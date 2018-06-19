package etl.spark;

import etl.common.annotation.ExcludeFromTest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
@ExcludeFromTest
public class Application {

    public static void main(String[] args) {
        new SpringApplicationBuilder(Application.class).web(false).run(args);
    }

}
