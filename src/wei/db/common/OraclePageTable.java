/**
 * 
 */
package wei.db.common;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * Pager for oracle data base.
 * @author Qin-Wei
 *
 */
public class OraclePageTable extends AbstractPageTable {
	
	/**log日志对象**/
	private static Logger log = Logger.getLogger(OraclePageTable.class);
	

	/* (non-Javadoc)
	 * @see wei.db.common.AbstractPageTable#getCountSql()
	 */
	@Override
	protected String getCountSql() {
		return "select count(*) from(" + this.sql + ")";
	}

	/* (non-Javadoc)
	 * @see wei.db.common.AbstractPageTable#getPageSql()
	 */
	@Override
	protected String getPageSql() {
		return "select * from (select rownum rn,ttquery.* from ("+sql+")ttquery where rownum<=?) where rn>?";
	}

	@Override
	protected PreparedStatement buildPageStatement() throws SQLException {
		String sql_s = getPageSql();
		PreparedStatement pst = getConnection().prepareStatement(sql_s);
		long maxrow=(currentPage - 1) * pageSize;
		long minrow=(currentPage)>1?(maxrow-pageSize):0;
		pst.setLong(1, maxrow);
		pst.setLong(2, minrow);
		log.info(sql_s+";pageNo:"+currentPage+";pageSize:"+pageSize);
		return pst;
	}

}
