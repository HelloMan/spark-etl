
package etl.common.annotation;

import org.apache.hadoop.classification.InterfaceStability;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to inform users of a package, class or method's intended audience.
 * Currently the audience can be {@link Public}, {@link LimitedPrivate} or
 * {@link Private}. <br>
 * All public classes must have InterfaceAudience annotation. <br>
 * <ul>
 * <li>Public classes that are not marked with this annotation must be
 * considered by default as {@link Private}.</li> 
 * 
 * <li>External applications must only use classes that are marked
 * {@link Public}. Avoid using non public classes as these classes
 * could be removed or change in incompatible ways.</li>
 * 
 * <li>Ignis projects must only use classes that are marked
 * {@link LimitedPrivate} or {@link Public}</li>
 * 
 * <li> Methods may have a different annotation that it is more restrictive
 * compared to the audience classification of the class. Example: A class 
 * might be {@link Public}, but a method may be {@link LimitedPrivate}
 * </li></ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceAudience {
  /**
   * Intended for use by any project or application.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Public {};
  
  /**
   * Intended only for the project(s) specified in the annotation.
   * For example, "client", "server", "staging", "pipeline".
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface LimitedPrivate {
    String[] value();
  };
  
  /**
   * Intended for use only within etl itself.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Private {};

  private InterfaceAudience() {} // Audience can't exist on its own
}
