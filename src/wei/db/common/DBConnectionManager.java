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
	/** mysql����DB, ֵΪ{@value} **/
	public static final int DB_TYPE_MYSQL = 0x1;

	/** oracle����DB, ֵΪ{@value} **/
	public static final int DB_TYPE_ORACLE = 0x2;

	private static HashMap<String, ComboPooledDataSource> dataSourceMap = new HashMap<String, ComboPooledDataSource>();

	/** ���ݿ�����, ������{@link #DB_TYPE_MYSQL}��{@link #DB_TYPE_ORACLE} **/
	public int dbType;

	private String dbname = "defaultdb";

	/** ������Connection�󶨵���ǰ�߳��ϵı��� **/
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
	 * ������Դ����ȡ��ָ�����Ƶ�����Դ.
	 * 
	 * @return ��ǰ���ݿ�����Ӧ������Դ.
	 */
	public synchronized DataSource getDataSource() {
		initConnPool(dbname);
		return dataSourceMap.get(dbname);
	}

	/**
	 * ����ǰʹ�õ����ݿ����趨��ָ�������ݿ���,���������г�ʼ��.
	 * 
	 * @param dbname
	 *            ���ݿ���
	 */
	public void setDbname(String dbname) {
		this.dbname = dbname;
		initConnPool(dbname);
	}

	/**
	 * ��ȡ��ǰ���ݿ���.
	 * 
	 * @return ��ǰ���ݿ���
	 */
	public String getDbname() {
		initConnPool(dbname);
		return dbname;
	}

	/**
	 * �������ݿ��Ĭ�����Ӳ�����ȡ���ݿ��Connection���󣬲��󶨵���ǰ�߳���
	 * 
	 * @return �ɹ�������Connection���󣬷��򷵻�null
	 */
	public synchronized Connection getConnection() {
		// �ȴӵ�ǰ�߳���ȡ������ʵ��
		Connection conn = threadSession.get();
		try {
			// ��ǰ�߳���û��Connection��ʵ��
			if (null == conn || conn.isClosed()) {
				DataSource dataSource = getDataSource();
				// �����ӳ���ȡ��һ������ʵ��
				conn = dataSource.getConnection();
				// �����󶨵���ǰ�߳���
				threadSession.set(conn);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * �ӵ�ǰ�߳��л�������Ѿ��󶨳ɹ���Connection����.
	 * 
	 * @return �ɹ�������Connection���󣬷���null
	 */
	public synchronized Connection localConnection() {
		return threadSession.get();
	}

	/**
	 * �������ݿ��Ĭ�����Ӳ�����ȡ���ݿ��Connection���󣬲��󶨵���ǰ�߳���
	 * 
	 * @return �ɹ�������Connection���󣬷��򷵻�null
	 */
	public synchronized Connection newConnection() {
		Connection conn = null;
		try {
			DataSource dataSource = getDataSource();
			// �����ӳ���ȡ��һ������ʵ��
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * �ر�session���ѱ�������ݿ�����,�����ǰ�߳�����connection����Nothing.
	 */
	public void close() {
		close(threadSession.get());
	}

	/**
	 * �ر����Ӳ��ͷ�session
	 * 
	 * @param conn
	 *            �ر�ָ��������
	 */
	public void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error("�ر�����ʱ�����쳣", e);
			} finally {
				/** жװ�̰߳� **/
				threadSession.remove();
			}
		}
	}

	/**
	 * �ع�����
	 * 
	 * @param conn
	 *            ����
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
