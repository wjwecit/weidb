/**
 * 
 */
package wei.db.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * @author Qin-Wei
 *
 */
public abstract class AbstractPageTable {
	/**log日志对象**/
	private static Logger log = Logger.getLogger(AbstractPageTable.class);
	
	/**查询结果总页数 **/
	private long totalPage = -1;
	
	/**返回字串数组格式之数据**/
	private ArrayList<HashMap<String,String>> dataArray;
	
	/**查询结果中总记录数**/
	private long totalRow = 0;
	
	/**分页大小**/
	protected int pageSize;
	
	/**当前页面中记录数**/
	private long currentPageRows = 0;
	
	/**当前页数**/
	protected long currentPage;
	
	/**查询SQL语句**/
	protected String sql;
	
	/**查询结果中记录总列数**/
	protected int columnCount = 1;
	
	/**标识查询是否成功**/
	private boolean inited = false;
	
	/**默认页面大小页面**/
	public static final int INIT_PAGE_SIZE = 10;
	
	/**最大页面大小**/
	public static final int MAX_PAGE_SIZE = 999;
	
	/**默认页数**/
	public static final int INIT_CURRENT_PAGE = 1;
	

	private String[] labels = null;
	
	private DBManager dbManager;

	/**
	 * 默认构造函数
	 */
	public AbstractPageTable() {
		this.currentPage = INIT_CURRENT_PAGE;
		this.pageSize = INIT_PAGE_SIZE;
	}

	public AbstractPageTable(String querysql, long currPageNo, int pagesize) {
		this.sql = querysql;
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
	
	protected void initCount(ResultSet rsc) throws NumberFormatException, SQLException{
		if (rsc != null) {
			while (rsc.next()) {
				Object rows_o = rsc.getObject(1);
				totalRow = Integer.parseInt(rows_o.toString());
				totalPage = (totalRow % pageSize == 0) ? totalRow / pageSize : (totalRow / pageSize) + 1;
				if (this.currentPage > totalPage) {
					this.currentPage = totalPage;
				}
				break;
			}
		}
	}

	/**
	 * 逻辑主体，对各参数进行正确赋值.
	 */
	public void rend() {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null, rsc = null;
		try {
			conn = this.getConnection();
			String sql_c = getCountSql();
			pst = conn.prepareStatement(sql_c);
			rsc = pst.executeQuery();
			
			/**开始渲染总记录数**/
			initCount(rsc);
			
			if (this.totalRow == 0) {
				log.info("no data found:" + this.sql);
				this.dataArray = null;
				return;
			}
			pst=buildPageStatement();
			rs = pst.executeQuery();
			if (rs != null) {
				ResultSetMetaData rmd = rs.getMetaData();
				int iCols = rmd.getColumnCount();
				setColumnCount(iCols);
				labels = new String[iCols];
				for (int col = 1; col <= iCols; col++) {
					labels[col - 1] = rmd.getColumnLabel(col);
				}
				dataArray = new ArrayList<HashMap<String,String>>();
				String scell = null;
				long rowc = 0;
				while (rs.next()) {
					HashMap<String,String> rowMap=new HashMap<String,String>();
					for (int i = 1; i <= iCols; i++) {
						scell = rs.getString(i);
						rowMap.put(labels[i-1], (scell == null ? "" : scell).trim());
					}
					rowMap.put("rownum", ++rowc+currentPage*pageSize-pageSize+"");
					dataArray.add(rowMap);
				}
				currentPageRows = rowc;
				inited = true;
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
			try {
				throw e;
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
		} finally {
			dbManager.close(conn);
		}
	}

	public long getTotalRow() {
		if (!isInited()) {
			return 0l;
		}
		return totalRow;
	}
	
	protected abstract String getCountSql();
	
	protected abstract String getPageSql();
	
	protected abstract PreparedStatement buildPageStatement() throws SQLException;

	/**
	 * 获得总页数，当结果集中记录数为0时，则返回0。
	 * 
	 * @return 总页数。
	 */
	public long getTotalPage() {
		if (!isInited()) {
			return 0;
		}
		return totalPage;
	}

	public long getCurrentPageRows() {
		if (!isInited()) {
			return 0l;
		}
		return this.currentPageRows;
	}

	public void setCurrentPage(long currentPage) {
		if (currentPage < 1) {
			currentPage = 1;
		}
		this.currentPage = currentPage;
	}

	/**
	 * 得到当前的页码,如果未进行任何操作,则返回默认页码.
	 * @return
	 */
	public long getCurrentPage() {
		if (!isInited()) {
			return INIT_CURRENT_PAGE;
		}
		if (this.currentPage > this.getTotalPage()) {
			this.currentPage = this.getTotalPage();
		}
		return this.currentPage;
	}

	/**
	 * 获得每页显示记录数,如果尚未进行任何操作,则返回默认的页面大小INIT_PAGE_SIZE
	 * @return 每页显示记录数
	 */
	public int getPageSize() {
		if (!isInited()) {
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

	public String[] getLabels() {
		if (!isInited()) {
			return null;
		}
		return this.labels;
	}

	protected Connection getConnection() {
		if(dbManager==null){
			dbManager=new DBManager();
		}
		return dbManager.getConnection();
	}

	public boolean isInited() {
		if (inited) {
			return true;
		}
		try {
			rend();			
		} catch (Exception ex) {
			log.error(ex.getMessage());
			inited = false;
		}
		return inited;
	}

	public void setInited(boolean isInited) {
		this.inited = isInited;
	}
	
	public ArrayList<HashMap<String, String>> getDataArray() {
		if (!isInited()) {
			return null;
		}
		return dataArray;
	}

	public void setDataArray(ArrayList<HashMap<String, String>> dataArray) {
		this.dataArray = dataArray;
	}

	public int getColumnCount() {
		if (!isInited()) {
			return 0;
		}
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}
}
