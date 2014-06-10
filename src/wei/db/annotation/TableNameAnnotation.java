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

	/** table name in database **/
	public String name() default "";

	/** auto mapping column to java bean properties **/
	public boolean autoMap() default true;
}
