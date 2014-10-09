/**
 * 
 */
package wei.db.common;


/**
 * Pager for oracle data base.
 * @author Qin-Wei
 *
 */
public class OraclePageTable extends PageTable {
	
	public OraclePageTable(Session session) {
		super(session);
	}
	
	/* (non-Javadoc)
	 * @see wei.db.common.AbstractPageTable#getCountSql()
	 */
	@Override
	protected String getCountSql() {
		if(sql==null||sql.isEmpty()){
			throw new RuntimeException("sql can not empty!");
		}
		return "select count(*) from(" + this.sql + ")";
	}

	/* (non-Javadoc)
	 * @see wei.db.common.AbstractPageTable#getPageSql()
	 */
	@Override
	protected String getPageSql() {
		if(sql==null||sql.isEmpty()){
			throw new RuntimeException("sql can not empty!");
		}
		return "select * from (select rownum rn,ttquery.* from ("+sql+")ttquery where rownum<=?) where rn>?";
	}

	@Override
	protected Object[] getPageParameters() {
		long maxrow=(currentPage - 1) * pageSize;
		long minrow=(currentPage)>1?(maxrow-pageSize):0;
		return new Object[]{maxrow,minrow};
	}
}
