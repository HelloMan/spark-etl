
package etl.common.annotation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time. Currently the stability can be
 * {@link Stable}, {@link Evolving} or {@link Unstable}. <br>
 * 
 * <ul><li>All classes that are annotated with {@link Public} or
 * {@link LimitedPrivate} must have InterfaceStability annotation. </li>
 * <li>Classes that are {@link Private} are to be considered unstable unless
 * a different InterfaceStability annotation states otherwise.</li>
 * <li>Incompatible changes must not be made to classes marked as stable.</li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceStability {
  /**
   * Can evolve while retaining compatibility for minor release boundaries.; 
   * can break compatibility only at major release (ie. at m.0).
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Stable {};
  
  /**
   * Evolving, but can break compatibility at minor release (i.e. m.x)
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Evolving {};
  
  /**
   * No guarantee is provided as to reliability or stability across any
   * level of release granularity.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Unstable {};
}
