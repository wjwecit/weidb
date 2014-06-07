/**
 * 
 */
package wei.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 注解方式注入实体类的表主键.
 * @author wei
 * @since 2014-3-12
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableKey {
	/**
	 * 主键的字段名,默认是空白
	 */
	public String columnName()default "";
}
