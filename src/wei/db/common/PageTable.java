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
	/** log��־���� **/
	private static Logger log = Logger.getLogger(PageTable.class);

	/** ��ѯ�����ҳ�� **/
	private long totalPage = -1;

	/** ��ѯ������ܼ�¼�� **/
	private long totalRow = 0;

	/** ��ҳ��С **/
	protected int pageSize;

	/** ��ǰҳ�� **/
	protected long currentPage;

	/** ��ѯSQL��� **/
	protected String sql;

	/** ��ʶ��ѯ�Ƿ�ɹ� **/
	private boolean inited = false;

	/** Ĭ��ҳ���Сҳ�� **/
	public static final int INIT_PAGE_SIZE = 10;

	/** ���ҳ���С **/
	public static final int MAX_PAGE_SIZE = 999;

	/** Ĭ��ҳ�� **/
	public static final int INIT_CURRENT_PAGE = 1;

	private List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();

	private Session session;

	/**
	 * Ĭ�Ϲ��캯��
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
	 * �߼����壬�Ը�����������ȷ��ֵ.
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
	 * �����ҳ������������м�¼��Ϊ0ʱ���򷵻�0��
	 * 
	 * @return ��ҳ����
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
	 * �õ���ǰ��ҳ��,���δ�����κβ���,�򷵻�Ĭ��ҳ��.
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
	 * ���ÿҳ��ʾ��¼��,�����δ�����κβ���,�򷵻�Ĭ�ϵ�ҳ���СINIT_PAGE_SIZE
	 * 
	 * @return ÿҳ��ʾ��¼��
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
