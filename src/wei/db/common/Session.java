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

import wei.db.annotation.TableKey;

/**
 * 数据库核心操作类, 依赖近似原生态的jdbc处理方式完成数据操作。
 * 
 * @author Qin-Wei
 * @since 2014-3-13
 */
public class Session {

	private static final Logger log = Logger.getLogger(Session.class);

	private DBManager dbManager;

	private Connection g_connection = null;

	private boolean isInTransaction = false;

	public Session() {
		dbManager = new DBManager();
	}

	public Session(Connection conn) {
		dbManager = new DBManager();
		g_connection = conn;
	}

	/**
	 * 获得当前线程中的绑定的sql connection.
	 * 
	 * @return Connection 对象
	 */
	public Connection getConnection() {
		return g_connection != null ? g_connection : dbManager.getConnection();
	}

	/**
	 * 开启一个事务,这会将现有的连接设置autoCommit=false,后续的操作直至遇到{@link #endTransaction} 时 才会进行
	 * 事务的commit或者rollback, 并关闭连接. 因此,一旦开启了事务,必须显式结束事务,否则
	 * 将引发连接泄漏.
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
	 * 配合{@link #beginTransaction()}进行使用,提交事务并关闭连接
	 */
	public void endTransaction() {
		dbManager.commitAndClose(g_connection);
		isInTransaction = true;
	}

	/**
	 * 将查询结果映射到实体Map中.
	 * 
	 * @param sql
	 *            执行查询的sql语句
	 * @return 如果执行记录集中有结果,将返回第一个结果并注入到Map中,如果无记录则返回null.
	 */
	public Map<String, Object> getMap(String sql) {
		QueryRunner run = new QueryRunner();
		MapHandler handler = new MapHandler();
		try {
			Map<String, Object> map = run.query(getConnection(), sql, handler);
			log.debug("query sql=" + sql + ";mapsize=" + map.size());
			return map;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (g_connection == null && !isInTransaction) {
				dbManager.close();
			}
		}
		return null;
	}

	/**
	 * 将查询结果映射到实体bean中的属性.
	 * 
	 * @param clz
	 *            bean类
	 * @param sql
	 *            执行查询的sql语句
	 * @return 如果执行记录集中有结果,将返回第一个结果并注入到bean中,如果无记录则返回null.
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
				dbManager.close();
			}
		}
		return null;
	}

	/**
	 * 将查询结果映射到实体bean中的属性.
	 * 
	 * @param clz
	 *            bean类
	 * @param sql
	 *            执行查询的sql语句
	 * @param params
	 *            sql参数
	 * @return 如果执行记录集中有结果,将返回第一个结果并注入到bean中,如果无记录则返回null.
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
				dbManager.close();
			}
		}
		return null;
	}

	/**
	 * 将查询结果注入到实体bean中的对应属性,并以列表的形式存放结果集.
	 * 
	 * @param clz
	 *            bean类
	 * @param sql
	 *            执行查询的sql语句
	 * @return 如果执行记录集中有结果,将返回结果并注入到bean中,如果无记录则返回null.
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
				dbManager.close();
			}
		}
		return null;
	}

	/**
	 * 将查询结果注入到实体Map中的对应属性,并以列表的形式存放结果集.
	 * 
	 * @param sql
	 *            执行查询的sql语句
	 * @return 如果执行记录集中有结果,将返回结果并注入到Map中,如果无记录则返回null.
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
				dbManager.close();
			}
		}
		return null;
	}

	/**
	 * 执行更新操作. 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param bean
	 *            即将被更新的实体bean.
	 * @param key
	 *            bean中的主键,可以不是真正的物理数据表主键,仅作为一个更新条件.
	 * @return 返回更新的记录行数,有可能给出的key存在多条匹配.
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
	 */
	public int update(Object bean, String key) throws SQLException {
		String tablename = bean.getClass().getSimpleName().toLowerCase();
		StringBuilder update = new StringBuilder("update " + tablename + " set ");
		Map<String, Object> props = mapBeanProperties(bean);
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
		return update(update.toString(), values.toArray());
	}

	/**
	 * 执行更新操作,必须在bean中添加主键注解<code>@TableKey</code>.
	 * 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param bean
	 *            即将被更新的实体bean.
	 * @return 返回更新的记录行数,有可能给出的key存在多条匹配.
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
	 */
	public int update(Object bean) throws SQLException {
		return update(bean, getKeyPropertyName(bean));
	}

	/**
	 * 执行插入操作. 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param bean
	 *            被操作的实体bean.
	 * @param aikey
	 *            数据表中的唯一一个自增长键名称
	 * @return 操作是否成功.
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
	 */
	public boolean insert(Object bean, String aikey) throws SQLException {
		String tablename = bean.getClass().getSimpleName();
		StringBuilder sqlk = new StringBuilder("insert into " + tablename + " ( ");
		StringBuilder sqlv = new StringBuilder("values( ");
		Map<String, Object> props = mapBeanProperties(bean);
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
		return update(sqlk.toString(), values.toArray()) > 0;
	}

	/**
	 * 执行插入操作. 必须在bean中添加主键注解<code>@TableKey</code>.
	 * 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param bean
	 *            被操作的实体bean.
	 * @return 操作是否成功.
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
	 */
	public boolean insert(Object bean) throws SQLException {
		return insert(bean, getKeyPropertyName(bean));
	}

	/**
	 * 使用?占位符执行更新操作,可以是update, insert, delete.
	 * 此方法务必放在doWithinTransaction方法中进行实施,以达到对事务的处理.
	 * 
	 * @param sql
	 *            要执行的原生态SQL语句.
	 * @param params
	 *            参数数组
	 * @return 影响的记录数
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
	 */
	public int update(String sql, Object[] params) throws SQLException {
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
				dbManager.close();
			}
		}
		return res;
	}

	/**
	 * 使用参数数组注入PreaparedStatement,即将参数数组替换语句中的?点位符.
	 * 
	 * @param pstmt
	 *            PreaparedStatement对象
	 * @param params
	 *            参数数组
	 * @throws SQLException
	 *             如果在执行中有SQL异常发生,则抛出.
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
	 * 将实体属性映射到map,必须符合java bean规范,拥有get读取方法．
	 * 
	 * @param bean实体
	 * @return map类型的映射
	 */
	private static Map<String, Object> mapBeanProperties(Object bean) {
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
					setterMap.put(propertyName, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			setterMap = null;
		}
		return setterMap;
	}

	/**
	 * 通过注解<code>@TableKey</code>得到数据表的主键字段名.<link>aa</link>
	 * 
	 * @param bean
	 *            实体
	 * @return 字串格式的主键字段名
	 */
	public static String getKeyPropertyName(Object bean) {
		String res = null;
		try {
			Field[] fields = bean.getClass().getDeclaredFields();
			for (Field field : fields) {
				TableKey annotation = field.getAnnotation(TableKey.class);
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
			throw new RuntimeException("Key must be set in the entity class.");
		}
		return res;
	}
}
