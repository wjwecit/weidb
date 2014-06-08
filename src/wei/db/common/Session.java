/**
 * 
 */
package wei.db.common;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
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

import wei.db.annotation.TableAIKeyAnnotation;
import wei.db.annotation.TableColumnAnnotation;
import wei.db.annotation.TableNameAnnotation;
import wei.db.annotation.TablePrimaryKeyAnnotation;

/**
 * ���ݿ���Ĳ�����, ��������ԭ��̬��jdbc����ʽ������ݲ�����
 * 
 * @author Qin-Wei
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
		dbManager.rollbackAndClose(g_connection);
		isInTransaction = false;
	}

	/**
	 * ���{@link #beginTransaction()}����ʹ��,�ع����񲢹ر�����
	 */
	public void rollbackcommit() {
		dbManager.commitAndClose(g_connection);
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
		return update(bean, getTablePrimaryKey(bean));
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
		return insert(bean, getTableAIKey(bean));
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
			throw new SQLException("Wrong number of parameters: expected " + stmtCount + ", was given " + paramsCount);
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
		HashMap<String, Object> setterMap = new HashMap<String, Object>();

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
					if (value == null) {
						continue;
					}
					setterMap.put(getTableColumn(bean, propertyName), value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			setterMap = null;
		}
		return setterMap;
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
		TableNameAnnotation annotation = clz.getDeclaredAnnotation(TableNameAnnotation.class);
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
	public static String getTablePrimaryKey(Object bean) {
		String res = null;
		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			for (Field field : fields) {
				TablePrimaryKeyAnnotation annotation = field.getAnnotation(TablePrimaryKeyAnnotation.class);
				if (annotation != null) {
					PropertyDescriptor properDescriptor = new PropertyDescriptor(field.getName(), bean.getClass());
					Method getter = properDescriptor.getReadMethod();
					if (getter != null) {
						TableColumnAnnotation annotation2 = field.getAnnotation(TableColumnAnnotation.class);
						res = annotation2.columnName().equals("") ? field.getName() : annotation2.columnName();
						return res;
					}
				}
			}
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		if (res == null || res.length() < 1) {
			throw new RuntimeException("Key must be set in the entity class.");
		}
		return res;
	}

	/**
	 * Get auto incrace key for the mapped data table where marked <code>@TableAIKeyAnnotation</code>
	 * 
	 * @param bean
	 *            the bean to be mapped
	 */
	public static String getTableAIKey(Object bean) {
		if (getTableName(bean.getClass()) == null) {
			throw new RuntimeException("Table name must be set in the entity class:" + bean.getClass());
		}
		String res = null;
		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			for (Field field : fields) {
				TableAIKeyAnnotation annotation = field.getAnnotation(TableAIKeyAnnotation.class);
				if (annotation != null) {
					PropertyDescriptor properDescriptor = new PropertyDescriptor(field.getName(), bean.getClass());
					Method getter = properDescriptor.getReadMethod();
					if (getter != null) {
						TableColumnAnnotation annotation2 = field.getAnnotation(TableColumnAnnotation.class);
						res = annotation2.columnName().equals("") ? field.getName() : annotation2.columnName();
						return res;
					}
				}
			}
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * Get the column name corresponding to the mapped bean. If there is no comumn mapped to this bean
	 * property, an runtime exception will be thrown.
	 * 
	 * @param bean
	 *            the mapped bean
	 * @param property
	 *            the property in the bean
	 * @return the column of the data table
	 */
	public static String getTableColumn(Object bean, String property) {
		if (getTableName(bean.getClass()) == null) {
			throw new RuntimeException("Table name must be set in the entity class:" + bean.getClass());
		}
		String res = null;
		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			for (Field field : fields) {
				if (!field.getName().equalsIgnoreCase(property)) {
					continue;
				}
				TableColumnAnnotation annotation = field.getAnnotation(TableColumnAnnotation.class);
				if (annotation != null) {
					PropertyDescriptor properDescriptor = new PropertyDescriptor(field.getName(), bean.getClass());
					Method getter = properDescriptor.getReadMethod();
					if (getter != null) {
						res = annotation.columnName().equals("") ? field.getName() : annotation.columnName();
						return res;
					}
				}
			}
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		if (res == null || res.length() < 1) {
			throw new RuntimeException("Comumn annotation must be set in the entity class:" + bean.getClass());
		}
		return res;
	}
}
