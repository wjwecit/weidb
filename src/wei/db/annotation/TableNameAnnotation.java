package wei.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Definite the table name corresponding to the data base table.
 * 
 * @author wei
 * @since 2014-06-08
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableNameAnnotation {
	public String name() default "";
}
