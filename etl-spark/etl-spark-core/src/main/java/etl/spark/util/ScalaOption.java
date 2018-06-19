package etl.spark.util;

import lombok.experimental.UtilityClass;
import scala.Option;

@UtilityClass
public class ScalaOption {
	public static final Option NONE = scala.Option.apply(null);


}