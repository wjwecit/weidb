/**
 * 
 */
package wei.db.common;

import java.sql.SQLException;

/**
 * 事务执行器,通过此接口完成事务的处理.
 * @author wei
 * @ 2014-3-12
 */
public interface TransactionExecutor {
	
	/**
	 * 开始执行事务体
	 * @throws SQLException
	 */
	public void execute() throws SQLException;

}
