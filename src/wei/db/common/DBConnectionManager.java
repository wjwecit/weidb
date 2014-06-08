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
 * 
 * @author wei 2014-03-21
 * 
 */
public class DBConnectionManager {

	private static final Logger log = Logger.getLogger(DBConnectionManager.class);
	/** mysql type DB, value is {@value} **/
	public static final int DB_TYPE_MYSQL = 0x1;

	/** oracle type DB, value is{@value} **/
	public static final int DB_TYPE_ORACLE = 0x2;

	/** default datasource **/
	private static DataSource dataSource;

	/** the type of data base, can be {@link #DB_TYPE_MYSQL}or {@link #DB_TYPE_ORACLE} **/
	public static int dbType;

	private volatile static boolean isInit = false;

	/** threadlocal to bind Connection **/
	private ThreadLocal<Connection> threadSession = new ThreadLocal<Connection>();

	static {
		try {
			initConnPool();
		} catch (Exception e) {
			log.error("Can not init connection pool," + e);
			e.printStackTrace();
		}
	}

	private synchronized static void initConnPool() {
		ComboPooledDataSource cpds = new ComboPooledDataSource("defaultdb");
		dataSource = cpds;
		isInit = true;
		String dbClass = cpds.getDriverClass().toLowerCase();
		if (dbClass.contains("mysql")) {
			dbType = DB_TYPE_MYSQL;
		} else if (dbClass.contains("oracle")) {
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
	 * Get connection from threadlocal firstly, if not exists, fetch new from poool.
	 * 
	 * @return a live connection if success,else null.
	 */
	public synchronized Connection getConnection() {

		/* try get it from threadlocal */
		Connection conn = threadSession.get();
		try {
			/* fetch new one form pool if no connection avaliable */
			if (null == conn || conn.isClosed()) {
				if (!isInit) {
					initConnPool();
				}
				if (dataSource != null) {
					conn = dataSource.getConnection();

					/* bin it to threadlocal */
					threadSession.set(conn);
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
	 * Always fetch a new connection from the pool, no matter there is any live one attatched threadlocal.
	 * 
	 * @return a new phisical connection.
	 */
	public Connection openNewConnection() {
		Connection conn = null;
		try {
			if (!isInit) {
				initConnPool();
			}
			if (dataSource != null) {
				conn = dataSource.getConnection();
			} else {
				log.error("Connection can not fetch from datasource!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * close connection binded to this threadlocal, if there is no connection bined yet, nothing will done.
	 */
	public void close() {
		close(threadSession.get());
	}

	/**
	 * close the connection specified.
	 * 
	 * @param conn
	 *            the connection to be closed
	 * 
	 */
	protected void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error("Close phisical connection error.", e);
			}
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
