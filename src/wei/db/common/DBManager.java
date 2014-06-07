/**
 * 
 */
package wei.db.common;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author wei
 * 
 */
public class DBManager {

	private static final Logger log = Logger.getLogger(DBManager.class);
	/** mysql类型DB, 值为{@value} **/
	public static final int DB_TYPE_MYSQL = 0x1;

	/** oracle类型DB, 值为{@value} **/
	public static final int DB_TYPE_ORACLE = 0x2;

	private static DataSource dataSource;

	/** 数据库类型, 可以是{@link #DB_TYPE_MYSQL}和{@link #DB_TYPE_ORACLE} **/
	public static int dbType;

	private volatile static boolean isInit = false;

	/** 用来把Connection绑定到当前线程上的变量 **/
	private ThreadLocal<Connection> threadSession = new ThreadLocal<Connection>();

	static {
		try {
			initConnPool();
		} catch (Exception e) {
			log.error("Can not init connection pool," + e.getLocalizedMessage());
			e.printStackTrace();
		}
	}

	private synchronized static void initConnPool() {
		ComboPooledDataSource cpds = new ComboPooledDataSource("weidb");
		dataSource = cpds;
		isInit = true;
		String dbClass = cpds.getDriverClass();
		if (dbClass.matches(".*\\.mysql\\..*")) {
			dbType = DB_TYPE_MYSQL;
		} else if (dbClass.matches(".*\\.oracle\\..*")) {
			dbType = DB_TYPE_ORACLE;
		} else {
			dbType = 0;
		}
		log.info("Connection pool init successfully. " + cpds.toString());
	}

	public DataSource getDataSource() {
		if (!isInit) {
			initConnPool();
		}
		return dataSource;
	}

	/**
	 * 根据数据库的默认连接参数获取数据库的Connection对象，并绑定到当前线程上
	 * 
	 * @return 成功，返回Connection对象，否则返回null
	 */
	public synchronized Connection getConnection() {
		Connection conn = threadSession.get(); // 先从当前线程上取出连接实例
		try {
			if (null == conn || conn.isClosed()) { // 如果当前线程上没有Connection的实例
				if (!isInit) {
					initConnPool();
				}
				if (dataSource != null) {
					conn = dataSource.getConnection(); // 从连接池中取出一个连接实例
					threadSession.set(conn); // 把它绑定到当前线程上
				} else {
					log.error("Connection can not fetch from datasource!");
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 关闭session中已保存的数据库连接,如果当前线程中无connection则做Nothing.
	 */
	public void close() {
		close(threadSession.get());
	}

	/**
	 * 关闭连接并释放session
	 * 
	 * @param conn
	 *            关闭指定的连接
	 */
	protected void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error("关闭连接时出现异常", e);
			} finally {
				/** 卸装线程绑定 **/
				threadSession.remove();
			}
		}
	}

	/**
	 * 提交事务并关闭 sql 连接。
	 * 
	 * @param conn
	 *            即将被关闭的连接
	 */
	protected void commitAndClose(Connection conn) {
		if (conn != null) {
			try {
				conn.commit();
			} catch (SQLException e) {
				log.error("提交事务时出现异常", e);
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error("关闭连接时出现异常", e);
				}
				/** 卸装线程绑定 **/
				threadSession.remove();
			}
		}
	}

}
