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

	/** 日志对象 **/
	private static final Logger log = Logger.getLogger(Session.class);

	/** 连接管理器 **/
	private DBConnectionManager dbManager;

	/** 内置的一个物理连接对象 **/
	private Connection g_connection = null;

	/** 事务开启标识 **/
	private boolean isInTransaction = false;
	
	/**
	 * 创建一个新的会话,内置的连接不会马上开启,直到发生一个数据库操作.
	 */
	public Session() {
		dbManager = new DBConnectionManager();
	}

	/**
	 * 使用给定的连接对象创建一个新的会话,内置的连接不会马上开启,直到发生一个数据库操作.
	 * 
	 * @param conn
	 *           给定的物理连接
	 */
	public Session(String dbname) {
		dbManager = new DBConnectionManager();
		dbManager.setDbname(dbname);
	}

	/**
	 * 检查并更新连接
	 */
	public void reConnect() {
		if (g_connection == null) {
			g_connection = dbManager.getConnection();
		}
	}

	/**
	 * 如果没有关联的事务,则释放连接资源;如果有事务,则什么都不做.
	 */
	private void checkClose() {
		if (g_connection != null && !isInTransaction) {
			dbManager.close(g_connection);
			g_connection = null;
		}
	}

	/**
	 * 显式开始一个事务,之后的代码必须显式提交或者回滚来结束事务, 否则将会导致连接泄漏问题.
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
	 * 提交事务并释放连接资源.
	 */
	public void commit() {
		dbManager.commitAndClose(g_connection);
		isInTransaction = false;
		g_connection = null;
	}

	/**
	 * 回滚事务并释放连接资源.
	 */
	public void rollback() {
		dbManager.rollbackAndClose(g_connection);
		isInTransaction = false;
		g_connection = null;
	}

	/**
	 * 将结果记录集映射到Map,不用注解.
	 * 
	 * @param sql
	 *            SQL语句
	 * @return 如果映射成功,返回Map,否则返回null;
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
	 * 将结果记录集映射到javabean,不用注解.
	 * 
	 * @param clz
	 *            javabean 类属性
	 * @param sql
	 *            SQL语句
	 * @return 如果映射成功,返回javabean,否则返回null;
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
	 * 将结果记录集映射到javabean,不用注解.
	 * 
	 * @param clz
	 *            javabean 类属性
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数数组
	 * @return 如果映射成功,返回javabean,否则返回null;
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
	 * 将结果记录集映射到javabean列表. 此方法必须先使用注解将javabean映射到数据表.
	 * 
	 * @param sql
	 *            SQL语句
	 * @return 如果映射成功,返回javabean的列表,否则返回null;
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
	 * 将结果记录集映射到Map列表. 此方法适合于,结果集的数据过于复杂,或者不打算进行注解映射时使用.
	 * 
	 * @param sql
	 *            SQL语句
	 * @return 如果映射成功,返回HashMap的列表,否则返回null;
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
	 * 使用javabean 作为参数进行方便的更新操作. javabean 必须提前使用Table注解作好映射.
	 * 
	 * @param bean
	 *            java bean.
	 * @param key
	 *            主键
	 * @return 受影响的行数
	 * @throws Exception
	 *             发生错误时抛出
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
	 * 更新一个实体对象,必须提前使用注解进行映射.
	 * 
	 * @param bean
	 *            实体javabean.
	 * @return 受影响行数
	 * @throws Exception
	 *             如果发生错误
	 */
	public int update(Object bean) throws Exception {
		return update(bean, getTablePrimaryKey(bean.getClass()));
	}

	/**
	 * 执行插入操作. 必须在bean中添加主键注解<code>@TableKey</code>.
	 * 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param bean
	 *            被操作的实体bean.
	 * @return 操作是否成功.
	 * @throws Exception
	 *             如果在执行中有任何异常发生,则抛出.
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
	 * 主键为自增长字段时,insert操作后若想返回键值,可以使用本方法.
	 * 
	 * @param bean
	 *            java bean.
	 * @return 操作成功时返回本次自增长值, 否则返回-1.
	 * @throws Exception
	 *             如果在执行中有任何异常发生,则抛出.
	 */
	public synchronized long getAIinsertId(Object bean) throws Exception {
		if (!insert(bean))
			return -1;
		String sql = "SELECT (AUTO_INCREMENT-1)as id FROM information_schema.tables  WHERE table_name='";
		sql = sql + getTableName(bean.getClass()) + "'";
		return getLong(sql);
	}

	/**
	 * 使用?点位符执行DML insert/update/delete语句.
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数数组
	 * @return 受影响行数
	 * @throws Exception
	 *             如果发生错误
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
	 * 将PreparedStatement中的参数占位符进行填充.
	 * 
	 * @param pstmt
	 *            PreaparedStatement
	 * @param params
	 *            参数数组
	 * @throws SQLException
	 *             如果发生SQL错误
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
	 * 得到初始值.
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
