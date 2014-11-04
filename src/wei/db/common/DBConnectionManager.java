/**
 * 
 */
package wei.db.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author wei
 * 
 */
public class DBConnectionManager {

	private static final Logger log = Logger.getLogger(DBConnectionManager.class);
	/** mysql类型DB, 值为{@value} **/
	public static final int DB_TYPE_MYSQL = 0x1;

	/** oracle类型DB, 值为{@value} **/
	public static final int DB_TYPE_ORACLE = 0x2;

	private static HashMap<String, ComboPooledDataSource> dataSourceMap = new HashMap<String, ComboPooledDataSource>();

	/** 数据库类型, 可以是{@link #DB_TYPE_MYSQL}和{@link #DB_TYPE_ORACLE} **/
	public int dbType;

	private String dbname = "defaultdb";

	/** 用来把Connection绑定到当前线程上的变量 **/
	private ThreadLocal<Connection> threadSession = new ThreadLocal<Connection>();

	private synchronized static void initConnPool(String dbname) {
		if (dataSourceMap.containsKey(dbname)) {
			return;
		}
		ComboPooledDataSource ds = new ComboPooledDataSource(dbname);
		dataSourceMap.put(dbname, ds);
		log.info("Connection pool init successfully. " + ds.toString());
	}

	public int getDbType() {
		if (!dataSourceMap.containsKey(dbname)) {
			initConnPool(dbname);
		}
		String dbClass = dataSourceMap.get(dbname).getDriverClass();
		if (dbClass.matches(".*\\.mysql\\..*")) {
			dbType = DB_TYPE_MYSQL;
		} else if (dbClass.matches(".*\\.oracle\\..*")) {
			dbType = DB_TYPE_ORACLE;
		} else {
			dbType = 0;
		}
		return dbType;
	}

	/**
	 * 从数据源池中取出指定名称的数据源.
	 * 
	 * @return 当前数据库名对应的数据源.
	 */
	public synchronized DataSource getDataSource() {
		initConnPool(dbname);
		return dataSourceMap.get(dbname);
	}

	/**
	 * 将当前使用的数据库名设定成指定的数据库名,并立即进行初始化.
	 * 
	 * @param dbname
	 *            数据库名
	 */
	public void setDbname(String dbname) {
		this.dbname = dbname;
		initConnPool(dbname);
	}

	/**
	 * 获取当前数据库名.
	 * 
	 * @return 当前数据库名
	 */
	public String getDbname() {
		initConnPool(dbname);
		return dbname;
	}

	/**
	 * 根据数据库的默认连接参数获取数据库的Connection对象，并绑定到当前线程上
	 * 
	 * @return 成功，返回Connection对象，否则返回null
	 */
	public synchronized Connection getConnection() {
		// 先从当前线程上取出连接实例
		Connection conn = threadSession.get();
		try {
			// 当前线程上没有Connection的实例
			if (null == conn || conn.isClosed()) {
				DataSource dataSource = getDataSource();
				// 从连接池中取出一个连接实例
				conn = dataSource.getConnection();
				// 把它绑定到当前线程上
				threadSession.set(conn);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 从当前线程中获得事先已经绑定成功的Connection对象.
	 * 
	 * @return 成功，返回Connection对象，否则null
	 */
	public synchronized Connection localConnection() {
		return threadSession.get();
	}

	/**
	 * 根据数据库的默认连接参数获取数据库的Connection对象，并绑定到当前线程上
	 * 
	 * @return 成功，返回Connection对象，否则返回null
	 */
	public synchronized Connection newConnection() {
		Connection conn = null;
		try {
			DataSource dataSource = getDataSource();
			// 从连接池中取出一个连接实例
			conn = dataSource.getConnection();
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
	public void close(Connection conn) {
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
	 * 回滚操作
	 * 
	 * @param conn
	 *            连接
	 */
	public void rollback(Connection conn) {
		try {
			conn.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Rollack and close the phisical specified connection.
	 * 
	 * @param conn
	 *            the connection to be closed
	 */
	protected void rollbackAndClose(Connection conn) {
		if (conn != null) {
			try {
				conn.rollback();
				conn.close();
			} catch (SQLException e) {
				log.error("Rollback and close phisical connection error.", e);
			}
		}
	}

	/**
	 * Commit and close the phisical specified connection.
	 * 
	 * @param conn
	 *            the connection to be closed
	 */
	protected void commitAndClose(Connection conn) {
		if (conn != null) {
			try {
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				log.error("Commit and close phisical connection error.", e);
			}
		}
	}
}
