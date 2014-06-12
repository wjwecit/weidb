package wei.db.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mapping java bean to data base table.
 * 
 * @author wangjunwei 2014-06-10
 * 
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {

	/** table name in data base **/
	public String name() default "";

	/** primary key name of the data base table **/
	public String key() default "";
}
