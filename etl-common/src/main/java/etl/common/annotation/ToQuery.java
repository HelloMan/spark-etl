package etl.common.annotation;

import java.lang.annotation.*;

/**
 * Created by chaojun on 17/11/25.
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface ToQuery {
}
