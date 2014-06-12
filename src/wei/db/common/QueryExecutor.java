/**
 * 
 */
package wei.db.common;

import java.sql.ResultSet;

/**
 * 查询执行器,可灵活地在查询体中应用.
 * 
 * @author wangjunwei 2014-06-05
 * 
 */
public interface QueryExecutor {

	/**
	 * 处理查询后的结果集
	 */
	public void result(ResultSet rs) throws Exception;

	/**
	 * 处理结果集中遇到异常时,该方法被调用,通常是事务的回滚操作
	 */
	public void exception() throws Exception;

	/**
	 * 处理结果集完成之后,finally被调用. 通常时对资源的关闭操作.
	 */
	public void after();
}
