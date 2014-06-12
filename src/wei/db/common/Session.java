/**
 * 
 */
package wei.db.common;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.log4j.Logger;

/**
 * ���ݿ�����������,�ṩ��ݵ����ݿ��������.
 * 
 * @author wei
 * @since 2014-3-13
 */
public class Session {

	private static final Logger log = Logger.getLogger(Session.class);

	private DBConnectionManager dbManager;

	private Connection g_connection = null;

	private boolean isInTransaction = false;

	public Session() {
		dbManager = new DBConnectionManager();
	}

	public Session(Connection conn) {
		dbManager = new DBConnectionManager();
		g_connection = conn;
	}

	/**
	 * ��õ�ǰ�߳��еİ󶨵�sql connection.
	 * 
	 * @return Connection ����
	 */
	public Connection getConnection() {
		if (g_connection == null) {
			g_connection = dbManager.getConnection();
		}
		return g_connection;
	}

	/**
	 * ����һ������,��Ὣ���е���������autoCommit=false,�����Ĳ���ֱ������{@link #endTransaction} ʱ �Ż����
	 * �����commit����rollback, ���ر�����. ���,һ������������,������ʽ��������,����
	 * ����������й©.
	 */
	public void beginTransaction() {
		try {
			if (g_connection == null || g_connection.isClosed()) {
				g_connection = dbManager.getConnection();
			}
			g_connection.setAutoCommit(false);
			isInTransaction = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ���{@link #beginTransaction()}����ʹ��,�ύ���񲢹ر�����
	 */
	public void commit() {
		dbManager.commitAndClose(g_connection);
		isInTransaction = false;
	}

	/**
	 * ���{@link #beginTransaction()}����ʹ��,�ع����񲢹ر�����
	 */
	public void rollback() {
		dbManager.rollbackAndClose(g_connection);
		isInTransaction = false;
	}

	/**
	 * ����ѯ���ӳ�䵽ʵ��Map��.
	 * 
	 * @param sql
	 *            ִ�в�ѯ��sql���
	 * @return ���ִ�м�¼�����н��,�����ص�һ�������ע�뵽Map��,����޼�¼�򷵻�null.
	 */
	public Map<String, Object> getMap(String sql) {
		QueryRunner run = new QueryRunner();
		MapHandler handler = new MapHandler();
		try {
			Map<String, Object> map = run.query(getConnection(), sql, handler);
			log.debug("query sql=" + sql + ";map=" + map);
			return map;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return null;
	}

	/**
	 * ����ѯ���ӳ�䵽ʵ��bean�е�����.
	 * 
	 * @param clz
	 *            bean��
	 * @param sql
	 *            ִ�в�ѯ��sql���
	 * @return ���ִ�м�¼�����н��,�����ص�һ�������ע�뵽bean��,����޼�¼�򷵻�null.
	 */
	public <T> T getBean(Class<T> clz, String sql) {

		QueryRunner run = new QueryRunner();
		ResultSetHandler<T> handler = new BeanHandler<T>(clz);
		try {
			T bean = run.query(getConnection(), sql, handler);
			log.debug("Query SQL:" + sql + ";bean=" + bean);
			return bean;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return null;
	}

	/**
	 * ����ѯ���ӳ�䵽ʵ��bean�е�����.
	 * 
	 * @param clz
	 *            bean��
	 * @param sql
	 *            ִ�в�ѯ��sql���
	 * @param params
	 *            sql����
	 * @return ���ִ�м�¼�����н��,�����ص�һ�������ע�뵽bean��,����޼�¼�򷵻�null.
	 */
	public <T> T getBean(Class<T> clz, String sql, Object[] params) {
		QueryRunner run = new QueryRunner();
		ResultSetHandler<T> handler = new BeanHandler<T>(clz);
		try {
			T bean = run.query(getConnection(), sql, handler, params);
			log.debug("Query SQL=" + sql + ";params=" + java.util.Arrays.toString(params) + ";bean=" + bean);
			return bean;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return null;
	}

	/**
	 * ����ѯ���ע�뵽ʵ��bean�еĶ�Ӧ����,�����б����ʽ��Ž����.
	 * 
	 * @param clz
	 *            bean��
	 * @param sql
	 *            ִ�в�ѯ��sql���
	 * @return ���ִ�м�¼�����н��,�����ؽ����ע�뵽bean��,����޼�¼�򷵻�null.
	 */
	public <T> List<T> getBeanList(Class<T> clz, String sql) {
		QueryRunner run = new QueryRunner();
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(clz);
		try {
			List<T> list = run.query(getConnection(), sql, handler);
			log.debug("query sql=" + sql + ";listsize=" + list.size());
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return null;
	}

	/**
	 * ����ѯ���ע�뵽ʵ��Map�еĶ�Ӧ����,�����б����ʽ��Ž����.
	 * 
	 * @param sql
	 *            ִ�в�ѯ��sql���
	 * @return ���ִ�м�¼�����н��,�����ؽ����ע�뵽Map��,����޼�¼�򷵻�null.
	 */
	public List<Map<String, Object>> getMapList(String sql) {
		QueryRunner run = new QueryRunner();
		MapListHandler handler = new MapListHandler();
		try {
			List<Map<String, Object>> list = run.query(getConnection(), sql, handler);
			log.debug("Query SQL=" + sql + ";listsize=" + list.size());
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return null;
	}

	/**
	 * ִ�и��²���. �˷�����ط���doWithinTransaction�����н���ʵʩ,�Դﵽ������Ĵ���.
	 * 
	 * @param bean
	 *            ���������µ�ʵ��bean.
	 * @param key
	 *            bean�е�����,���Բ����������������ݱ�����,����Ϊһ����������.
	 * @return ���ظ��µļ�¼����,�п��ܸ�����key���ڶ���ƥ��.
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	public int update(Object bean, String key) throws SQLException {
		String tablename = bean.getClass().getSimpleName().toLowerCase();
		StringBuilder update = new StringBuilder("update " + tablename + " set ");
		Map<String, Object> props = getTableMapFromBean(bean);
		ArrayList<Object> values = new ArrayList<Object>();
		Object keyValue = null;
		for (Map.Entry<String, Object> e : props.entrySet()) {
			if (!e.getKey().equalsIgnoreCase(key)) {
				update.append(e.getKey() + "=?,");
				values.add(e.getValue());
			} else {
				keyValue = e.getValue();
			}
		}
		if (keyValue == null) {
			throw new SQLException("Can not update, key value must be set.");
		}
		values.add(keyValue);
		int index = update.lastIndexOf(",");
		update.replace(index, update.length(), " ");
		update.append("where " + key + "=?");
		return executeUpdate(update.toString(), values.toArray());
	}

	/**
	 * ִ�и��²���,������bean���������ע��<code>@TableKey</code>.
	 * �˷�����ط���doWithinTransaction�����н���ʵʩ,�Դﵽ������Ĵ���.
	 * 
	 * @param bean
	 *            ���������µ�ʵ��bean.
	 * @return ���ظ��µļ�¼����,�п��ܸ�����key���ڶ���ƥ��.
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	public int update(Object bean) throws SQLException {
		return update(bean, getTablePrimaryKey(bean.getClass()));
	}

	/**
	 * Insert into data table. AI��key should be specified
	 * 
	 * @param bean
	 *            ��������ʵ��bean.
	 * @param aikey
	 *            ���ݱ��е�Ψһһ��������������
	 * @return �����Ƿ�ɹ�.
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	public boolean insert(Object bean, String aikey) throws SQLException {
		String tablename = getTableName(bean.getClass());
		StringBuilder sqlk = new StringBuilder("insert into " + tablename + " ( ");
		StringBuilder sqlv = new StringBuilder("values( ");
		Map<String, Object> props = getTableMapFromBean(bean);
		ArrayList<Object> values = new ArrayList<Object>();
		for (Map.Entry<String, Object> e : props.entrySet()) {
			if (!e.getKey().equalsIgnoreCase(aikey)) {
				sqlk.append(e.getKey() + ",");
				sqlv.append("?,");
				values.add(e.getValue());
			}
		}
		int indexk = sqlk.lastIndexOf(",");
		int indexv = sqlv.lastIndexOf(",");
		if (indexk < 0 || indexv < 0) {
			throw new RuntimeException("Can not insert, value must be set.");
		}
		sqlk.replace(indexk, sqlk.length(), ")");
		sqlv.replace(indexv, sqlv.length(), ")");
		sqlk.append(sqlv);
		return executeUpdate(sqlk.toString(), values.toArray()) > 0;
	}

	/**
	 * ִ�в������. ������bean���������ע��<code>@TableKey</code>.
	 * �˷�����ط���doWithinTransaction�����н���ʵʩ,�Դﵽ������Ĵ���.
	 * 
	 * @param bean
	 *            ��������ʵ��bean.
	 * @return �����Ƿ�ɹ�.
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	public boolean insert(Object bean) throws SQLException {
		String tablename = getTableName(bean.getClass());
		StringBuilder sqlk = new StringBuilder("insert into " + tablename + "(");
		StringBuilder sqlv = new StringBuilder("values(");
		Map<String, Object> props = getTableMapFromBean(bean);
		ArrayList<Object> values = new ArrayList<Object>();
		for (Map.Entry<String, Object> e : props.entrySet()) {
			sqlk.append(e.getKey() + ",");
			sqlv.append("?,");
			values.add(e.getValue());
		}
		int indexk = sqlk.lastIndexOf(",");
		int indexv = sqlv.lastIndexOf(",");
		if (indexk < 0 || indexv < 0) {
			throw new RuntimeException("Can not insert, value must be set.");
		}
		sqlk.replace(indexk, sqlk.length(), ")");
		sqlv.replace(indexv, sqlv.length(), ")");
		sqlk.append(sqlv);
		return executeUpdate(sqlk.toString(), values.toArray()) > 0;
	}

	/**
	 * Take '?' placeholder to execute DML SQL statement(update, insert, delete).
	 * 
	 * @param sql
	 *            the SQL statement.
	 * @param params
	 *            the parameters array
	 * @return how many rows effected
	 * @throws SQLException
	 *             if there is any SQLException, popup it.
	 */
	public int executeUpdate(String sql, Object[] params) throws SQLException {
		int res = 0;
		Connection conn = getConnection();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			fillStatement(pstmt, params);
			res = pstmt.executeUpdate();
			if (pstmt != null) {
				pstmt.close();
			}
			log.debug("Update:" + res + "; SQL=" + sql + ";params=" + java.util.Arrays.toString(params) + ";");
		} catch (SQLException e) {
			throw e;
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close(g_connection);
			}
		}
		return res;
	}

	/**
	 * ʹ�ò�������ע��PreaparedStatement,�������������滻����е�?��λ��.
	 * 
	 * @param pstmt
	 *            PreaparedStatement����
	 * @param params
	 *            ��������
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	private static void fillStatement(PreparedStatement pstmt, Object[] params) throws SQLException {
		ParameterMetaData pmd = null;
		pmd = pstmt.getParameterMetaData();
		int stmtCount = pmd.getParameterCount();
		int paramsCount = params == null ? 0 : params.length;

		if (stmtCount != paramsCount) {
			throw new SQLException("Wrong number of parameters: expected " + stmtCount + ", while given " + paramsCount);
		}
		if (params == null) {
			return;
		}
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				pstmt.setObject(i + 1, params[i]);
			} else {
				int sqlType = 12;
				try {
					sqlType = pmd.getParameterType(i + 1);
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
				pstmt.setNull(i + 1, sqlType);
			}
		}
	}

	/**
	 * Map bean properties into map object. All of the properties must follow standard java bean specification.
	 * 
	 * @param bean
	 *            the bean to be maped
	 * @return the maped java bean
	 */
	public static Map<String, Object> getTableMapFromBean(Object bean) {
		HashMap<String, Object> beanMap = new HashMap<String, Object>();

		Class<? extends Object> clz = bean.getClass();
		Table tableAnnotation = clz.getAnnotation(Table.class);
		if (tableAnnotation == null) {
			throw new RuntimeException("Table name must be set in the entity class.");
		}
		try {
			BeanInfo info = Introspector.getBeanInfo(bean.getClass());
			PropertyDescriptor[] descritors = info.getPropertyDescriptors();
			int size = descritors.length;
			for (int index = 0; index < size; index++) {
				if (descritors[index].getName().equalsIgnoreCase("class")) {
					continue;
				}
				String propertyName = descritors[index].getName();
				Method method = descritors[index].getReadMethod();
				if (method != null) {
					Object value = method.invoke(bean, new Object[] {});
					if (value == null || value.equals(getInitialVlue(value))) {
						continue;
					}
					beanMap.put(propertyName, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			beanMap = null;
		}
		return beanMap;
	}

	/**
	 * Get table name where marked <code>@TableNameAnnotation</code> in bean definition.
	 * 
	 * @param clz
	 *            the name of bean
	 * @return the name of the data table
	 */

	public static String getTableName(Class<?> clz) {
		String res = null;
		Table annotation = clz.getAnnotation(Table.class);
		if (annotation != null) {
			res = annotation.name().equals("") ? clz.getSimpleName() : annotation.name();
		}
		if (res == null || res.length() < 1) {
			throw new RuntimeException("Table name must be set in the entity class.");
		}
		return res;
	}

	/**
	 * ͨ��ע��<code>@TablePrimaryKeyAnnotation</code>�õ����ݱ�������ֶ���.<link>aa</link>
	 * 
	 * @param bean
	 *            ʵ��
	 * @return �ִ���ʽ�������ֶ���
	 */
	public static String getTablePrimaryKey(Class<?> clz) {
		String res = null;
		Table annotation = clz.getAnnotation(Table.class);
		if (annotation != null) {
			res = annotation.key().equals("") ? clz.getSimpleName() : annotation.key();
		}
		if (res == null || res.length() < 1) {
			throw new RuntimeException("Table name must be set in the entity class.");
		}
		return res;
	}

	/**
	 * Get the initial value for the specified object.
	 * 
	 * @param obj
	 *            the object to be valued
	 * @return the corresponding initial value for this object
	 */
	public static Object getInitialVlue(Object obj) {
		if (obj == null) {
			return obj;
		}
		Class<? extends Object> type = obj.getClass();
		if (type == Boolean.TYPE || type == Boolean.class)
			return false;
		if (type == Byte.TYPE || type == Byte.class)
			return new Byte((byte) 0);
		if (type == Character.TYPE || type == Character.class)
			return new Character('\000');
		if (type == Double.TYPE || type == Double.class)
			return new Double(0.0D);
		if (type == Float.TYPE || type == Float.class)
			return new Float(0.0F);
		if (type == Integer.TYPE || type == Integer.class)
			return new Integer(0);
		if (type == Long.TYPE || type == Long.class)
			return new Long(0L);
		if (type == Short.TYPE || type == Short.class) {
			return new Short((short) 0);
		}
		return null;
	}
	
	/**
	 * ִ�в������. ������bean���������ע��<code>@TableKey</code>.
	 * 
	 * @param bean
	 *            ��������ʵ��bean.
	 * @return �����Ƿ�ɹ�.
	 * @throws SQLException
	 *             �����ִ������SQL�쳣����,���׳�.
	 */
	public synchronized long AIinsert(Object bean) throws SQLException {
		if (!insert(bean))
			return -1;
		return getLong("SELECT (AUTO_INCREMENT-1)as id FROM information_schema.tables  WHERE table_name='" + getTableName(bean.getClass()) + "'");
	}
	
	/**
	 * ����λ����ʽ�õ�������е�һ�е�һ�е�Long������. ��������ݽ�������.
	 * 
	 * @param sql
	 *            SQL���,�����Ǵ�?�ĵ�λ��
	 * @param params
	 *            �����б�
	 * @return �����ͽ��
	 */
	public long getLong(String sql, Object[] params) {
		Connection conn = getConnection();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			fillStatement(pstmt, params);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				return rs.getLong(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (!isInTransaction) {
				dbManager.close();
			}
		}
		return 0L;
	}
	
	
	
	
	/**
	 * �õ�������е�һ�е�һ�е�Long������. ��������ݽ�������.
	 * 
	 * @param sql
	 *            SQL���
	 * @return �����ͽ��
	 */
	public long getLong(String sql) {
		return getLong(sql, null);
	}


	/**
	 * �������ݱ��������ʱר�÷���. ʹ�ô˷���ʱ,��¼��������Ҫ��ȡ,������һ����ȫ��װ���ڴ�.<br>
	 * ������Ӧ�����ݼ��ϴ������ʹ�ñ�����,�Ӷ�����OOM�쳣����.
	 * 
	 * @param sql
	 *            ִ�е�SQL
	 * @param executor
	 *            ��ѯ�������ӿ�,ʵ�ִ˽ӿ�,���Խ�����Ĵ���.
	 */
	public void hugeQuery(String sql, QueryExecutor executor) {
		Connection conn =dbManager.newConnection();
		try {
			PreparedStatement pst = conn.prepareStatement(
					sql,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			pst.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = pst.executeQuery();
			executor.result(rs);
		} catch (Exception e) {
			try {
				e.printStackTrace();
				executor.exception();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			executor.after();
			dbManager.close(conn);
		}
	}}
