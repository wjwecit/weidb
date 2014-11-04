/**
 * 
 */
package wei.db.common;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
 * Core helper class to simplify jdbc operations. Depend on the apache dbutils
 * libary.
 * 
 * @author wei
 * @since 2014-3-13
 */
public class Session {

	/** ��־���� **/
	private static final Logger log = Logger.getLogger(Session.class);

	/** ���ӹ����� **/
	private DBConnectionManager dbManager;

	/** ���õ�һ���������Ӷ��� **/
	private Connection g_connection = null;

	/** ��������ʶ **/
	private boolean isInTransaction = false;
	
	/**
	 * ����һ���µĻỰ,���õ����Ӳ������Ͽ���,ֱ������һ�����ݿ����.
	 */
	public Session() {
		dbManager = new DBConnectionManager();
	}

	/**
	 * ʹ�ø��������Ӷ��󴴽�һ���µĻỰ,���õ����Ӳ������Ͽ���,ֱ������һ�����ݿ����.
	 * 
	 * @param conn
	 *           ��������������
	 */
	public Session(String dbname) {
		dbManager = new DBConnectionManager();
		dbManager.setDbname(dbname);
	}

	/**
	 * ��鲢��������
	 */
	public void reConnect() {
		if (g_connection == null) {
			g_connection = dbManager.getConnection();
		}
	}

	/**
	 * ���û�й���������,���ͷ�������Դ;���������,��ʲô������.
	 */
	private void checkClose() {
		if (g_connection != null && !isInTransaction) {
			dbManager.close(g_connection);
			g_connection = null;
		}
	}

	/**
	 * ��ʽ��ʼһ������,֮��Ĵ��������ʽ�ύ���߻ع�����������, ���򽫻ᵼ������й©����.
	 */
	public void beginTransaction() {
		if (isInTransaction) {
			rollback();
			throw new IllegalAccessError("Thread :" + Thread.currentThread().getName()
					+ " had already in transaction, must end it first!");
		}
		try {
			reConnect();
			g_connection.setAutoCommit(false);
			isInTransaction = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * �ύ�����ͷ�������Դ.
	 */
	public void commit() {
		dbManager.commitAndClose(g_connection);
		isInTransaction = false;
		g_connection = null;
	}

	/**
	 * �ع������ͷ�������Դ.
	 */
	public void rollback() {
		dbManager.rollbackAndClose(g_connection);
		isInTransaction = false;
		g_connection = null;
	}

	/**
	 * �������¼��ӳ�䵽Map,����ע��.
	 * 
	 * @param sql
	 *            SQL���
	 * @return ���ӳ��ɹ�,����Map,���򷵻�null;
	 */
	public Map<String, Object> getMap(String sql) {
		QueryRunner run = new QueryRunner();
		MapHandler handler = new MapHandler();
		try {
			reConnect();
			Map<String, Object> map = run.query(g_connection, sql, handler);
			log.debug("sql=" + sql + ";map=" + map);
			return map;
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return null;
	}

	/**
	 * �������¼��ӳ�䵽javabean,����ע��.
	 * 
	 * @param clz
	 *            javabean ������
	 * @param sql
	 *            SQL���
	 * @return ���ӳ��ɹ�,����javabean,���򷵻�null;
	 */
	public <T> T getBean(Class<T> clz, String sql) {
		QueryRunner run = new QueryRunner();
		ResultSetHandler<T> handler = new BeanHandler<T>(clz);
		try {
			reConnect();
			T bean = run.query(g_connection, sql, handler);
			log.debug("sql:" + sql + ";bean=" + bean);
			return bean;
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return null;
	}

	/**
	 * �������¼��ӳ�䵽javabean,����ע��.
	 * 
	 * @param clz
	 *            javabean ������
	 * @param sql
	 *            SQL���
	 * @param params
	 *            SQL��������
	 * @return ���ӳ��ɹ�,����javabean,���򷵻�null;
	 */
	public <T> T getBean(Class<T> clz, String sql, Object[] params) {
		QueryRunner run = new QueryRunner();
		ResultSetHandler<T> handler = new BeanHandler<T>(clz);
		try {
			reConnect();
			T bean = run.query(g_connection, sql, handler, params);
			log.debug("sql=" + sql + ";params=" + java.util.Arrays.toString(params) + ";bean=" + bean);
			return bean;
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return null;
	}

	/**
	 * �������¼��ӳ�䵽javabean�б�. �˷���������ʹ��ע�⽫javabeanӳ�䵽���ݱ�.
	 * 
	 * @param sql
	 *            SQL���
	 * @return ���ӳ��ɹ�,����javabean���б�,���򷵻�null;
	 */
	public <T> List<T> getBeanList(Class<T> clz, String sql) {
		QueryRunner run = new QueryRunner();
		ResultSetHandler<List<T>> handler = new BeanListHandler<T>(clz);
		try {
			reConnect();
			List<T> list = run.query(g_connection, sql, handler);
			log.debug("sql=" + sql + ";listsize=" + list.size());
			return list;
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return null;
	}

	/**
	 * �������¼��ӳ�䵽Map�б�. �˷����ʺ���,����������ݹ��ڸ���,���߲��������ע��ӳ��ʱʹ��.
	 * 
	 * @param sql
	 *            SQL���
	 * @return ���ӳ��ɹ�,����HashMap���б�,���򷵻�null;
	 */
	public List<Map<String, Object>> getMapList(String sql,Object[]params) {
		QueryRunner run = new QueryRunner();
		MapListHandler handler = new MapListHandler();
		try {
			reConnect();
			List<Map<String, Object>> list = run.query(g_connection, sql, handler,params);
			log.debug("sql=" + sql + ";params=" + java.util.Arrays.toString(params) + ";listsize=" + list.size());
			return list;
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return null;
	}

	/**
	 * Fetch long value for first line first column in the resultset.
	 * 
	 * @param sql
	 *            sql statement
	 * @return long value, if there is no such a result or there is any error
	 *         return 0L.
	 */
	public long getLong(String sql) {
		return getLong(sql, null);
	}

	/**
	 * Fetch long value for first line first column in the resultset.
	 * 
	 * @param sql
	 *            sql statement with spaceholders '?'
	 * @param params
	 *            parameter array
	 * @return long value, if there is no such a result or there is any error
	 *         return 0L.
	 */
	public long getLong(String sql, Object[] params) {
		try {
			reConnect();
			PreparedStatement pstmt = g_connection.prepareStatement(sql);
			fillStatement(pstmt, params);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				return rs.getLong(1);
			}
		} catch (SQLException e) {
			isInTransaction=false;
			e.printStackTrace();
		} finally {
			checkClose();
		}
		return 0L;
	}

	/**
	 * ʹ��javabean ��Ϊ�������з���ĸ��²���. javabean ������ǰʹ��Tableע������ӳ��.
	 * 
	 * @param bean
	 *            java bean.
	 * @param key
	 *            ����
	 * @return ��Ӱ�������
	 * @throws Exception
	 *             ��������ʱ�׳�
	 * 
	 */
	public int update(Object bean, String key) throws Exception {
		String tablename = getTableName(bean.getClass());
		StringBuilder update = new StringBuilder("update " + tablename + " set ");
		Map<String, Object> props = orm(bean);
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
	 * ����һ��ʵ�����,������ǰʹ��ע�����ӳ��.
	 * 
	 * @param bean
	 *            ʵ��javabean.
	 * @return ��Ӱ������
	 * @throws Exception
	 *             �����������
	 */
	public int update(Object bean) throws Exception {
		return update(bean, getTablePrimaryKey(bean.getClass()));
	}

	/**
	 * ִ�в������. ������bean���������ע��<code>@TableKey</code>.
	 * �˷�����ط���doWithinTransaction�����н���ʵʩ,�Դﵽ������Ĵ���.
	 * 
	 * @param bean
	 *            ��������ʵ��bean.
	 * @return �����Ƿ�ɹ�.
	 * @throws Exception
	 *             �����ִ�������κ��쳣����,���׳�.
	 */
	public boolean insert(Object bean) throws Exception {
		String tablename = getTableName(bean.getClass());
		StringBuilder sqlk = new StringBuilder("insert into " + tablename + "(");
		StringBuilder sqlv = new StringBuilder("values(");
		Map<String, Object> props = orm(bean);
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
	 * ����Ϊ�������ֶ�ʱ,insert���������뷵�ؼ�ֵ,����ʹ�ñ�����.
	 * 
	 * @param bean
	 *            java bean.
	 * @return �����ɹ�ʱ���ر���������ֵ, ���򷵻�-1.
	 * @throws Exception
	 *             �����ִ�������κ��쳣����,���׳�.
	 */
	public synchronized long getAIinsertId(Object bean) throws Exception {
		if (!insert(bean))
			return -1;
		String sql = "SELECT (AUTO_INCREMENT-1)as id FROM information_schema.tables  WHERE table_name='";
		sql = sql + getTableName(bean.getClass()) + "'";
		return getLong(sql);
	}

	/**
	 * ʹ��?��λ��ִ��DML insert/update/delete���.
	 * 
	 * @param sql
	 *            SQL���
	 * @param params
	 *            SQL��������
	 * @return ��Ӱ������
	 * @throws Exception
	 *             �����������
	 */
	public int executeUpdate(String sql, Object[] params) throws Exception {
		int res = 0;
		try {
			reConnect();
			PreparedStatement pstmt = g_connection.prepareStatement(sql);
			fillStatement(pstmt, params);
			res = pstmt.executeUpdate();
			if (pstmt != null) {
				pstmt.close();
			}
			log.debug("Update:" + res + "; sql=" + sql + ";params=" + java.util.Arrays.toString(params) + ";");
		} catch (SQLException e) {
			isInTransaction=false;
			throw e;
		} finally {
			checkClose();
		}
		return res;
	}
	
	public PageTable getTable(){
		if(dbManager.getDbType()==DBConnectionManager.DB_TYPE_MYSQL){
			return new MysqlPageTable(this);
		}else if(dbManager.getDbType()==DBConnectionManager.DB_TYPE_ORACLE){
			return new OraclePageTable(this);
		}
		return null;
	}

	/**
	 * ��PreparedStatement�еĲ���ռλ���������.
	 * 
	 * @param pstmt
	 *            PreaparedStatement
	 * @param params
	 *            ��������
	 * @throws SQLException
	 *             �������SQL����
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
	 * Map bean properties into map object. All of the properties must follow
	 * standard java bean specification.
	 * 
	 * @param bean
	 *            the bean to be maped
	 * @return the maped java bean
	 */
	public static Map<String, Object> orm(Object bean) {
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
				String propertyName = descritors[index].getName();
				if (propertyName.equalsIgnoreCase("class")) {
					continue;
				}
				Method method = descritors[index].getReadMethod();
				if (method != null && method.getModifiers() == Modifier.PUBLIC) {
					Object value = method.invoke(bean, new Object[] {});
					if (value == null) {
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
	 * Get table name where marked <code>@TableNameAnnotation</code> in bean
	 * definition.
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
	 * Get primary key for data base table via annotation
	 * 
	 * @param bean
	 *            java bean object
	 * @return primary key name.
	 */
	public static String getTablePrimaryKey(Class<?> clz) {
		String res = null;
		Table annotation = clz.getAnnotation(Table.class);
		if (annotation != null) {
			res = annotation.key();
		}
		if (res == null || res.length() < 1) {
			throw new RuntimeException("Table name must be set in the entity class.");
		}
		return res;
	}

	/**
	 * �õ���ʼֵ.
	 */
	public static Object getInitialVlue(Object obj) {
		if (obj == null) {
			return null;
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
}
