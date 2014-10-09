/**
 * 
 */
package wei.db.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author Qin-Wei
 * 
 */
public abstract class PageTable {
	/** log日志对象 **/
	private static Logger log = Logger.getLogger(PageTable.class);

	/** 查询结果总页数 **/
	private long totalPage = -1;

	/** 查询结果中总记录数 **/
	private long totalRow = 0;

	/** 分页大小 **/
	protected int pageSize;

	/** 当前页数 **/
	protected long currentPage;

	/** 查询SQL语句 **/
	protected String sql;

	/** 标识查询是否成功 **/
	private boolean inited = false;

	/** 默认页面大小页面 **/
	public static final int INIT_PAGE_SIZE = 10;

	/** 最大页面大小 **/
	public static final int MAX_PAGE_SIZE = 999;

	/** 默认页数 **/
	public static final int INIT_CURRENT_PAGE = 1;

	private List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();

	private Session session;

	/**
	 * 默认构造函数
	 */
	public PageTable(Session session) {
		this.session = session;
		currentPage = INIT_CURRENT_PAGE;
		pageSize = INIT_PAGE_SIZE;
	}

	public PageTable() {
		session = new Session();
		currentPage = INIT_CURRENT_PAGE;
		pageSize = INIT_PAGE_SIZE;
	}

	public PageTable(String sql, long currPageNo, int pagesize) {
		this.sql = sql;
		if (currPageNo < 1) {
			currPageNo = INIT_CURRENT_PAGE;
		}
		this.currentPage = currPageNo;
		if (pagesize < 1 || pagesize > 999) {
			pagesize = INIT_PAGE_SIZE;
		}
		this.pageSize = pagesize;
		rend();
	}

	/**
	 * 逻辑主体，对各参数进行正确赋值.
	 */
	public boolean rend() {
		if (inited) {
			return true;
		}
		try {
			String sqlTotalCount = getCountSql();
			totalRow = session.getLong(sqlTotalCount);
			totalPage = (totalRow % pageSize == 0) ? totalRow / pageSize : (totalRow / pageSize) + 1;
			if (currentPage > totalPage) {
				currentPage = totalPage;
			}
			dataList = session.getMapList(getPageSql(), getPageParameters());
			inited = true;
		} catch (Exception e) {
			log.error(e);
			inited = false;
		}

		return inited;
	}

	public long getTotalRow() {
		if (!rend()) {
			return 0l;
		}
		return totalRow;
	}

	public List<Map<String, Object>> getDataList() {
		if (!rend()) {
			return null;
		}
		return dataList;
	}

	protected abstract String getCountSql();

	protected abstract String getPageSql();

	protected abstract Object[] getPageParameters();

	/**
	 * 获得总页数，当结果集中记录数为0时，则返回0。
	 * 
	 * @return 总页数。
	 */
	public long getTotalPage() {
		if (!rend()) {
			return 0;
		}
		return totalPage;
	}

	public void setCurrentPage(long currentPage) {
		if (currentPage < 1) {
			currentPage = 1;
		}
		this.currentPage = currentPage;
	}

	/**
	 * 得到当前的页码,如果未进行任何操作,则返回默认页码.
	 * 
	 * @return
	 */
	public long getCurrentPage() {
		if (!rend()) {
			return INIT_CURRENT_PAGE;
		}
		if (this.currentPage > this.getTotalPage()) {
			this.currentPage = this.getTotalPage();
		}
		return this.currentPage;
	}

	/**
	 * 获得每页显示记录数,如果尚未进行任何操作,则返回默认的页面大小INIT_PAGE_SIZE
	 * 
	 * @return 每页显示记录数
	 */
	public int getPageSize() {
		if (!rend()) {
			return INIT_PAGE_SIZE;
		}
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		if (pageSize < 1 || pageSize > 999) {
			pageSize = INIT_PAGE_SIZE;
		}
		this.pageSize = pageSize;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
}
