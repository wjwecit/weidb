package wei.db.common;


/**
 * @author Wangjw 显示分页表格数据，使用原生态的SQL统计，并返回数组形式之数据。
 */
public class MysqlPageTable extends PageTable{
	
	public MysqlPageTable(Session session){
		super(session);
	}

	protected String getCountSql(){
		if(sql==null||sql.isEmpty()){
			throw new RuntimeException("sql can not empty!");
		}
		return "select count(*) from(" + this.sql + ")ttcount";
	}
	
	@Override
	protected String getPageSql() {
		if(sql==null||sql.isEmpty()){
			throw new RuntimeException("sql can not empty!");
		}
		return "select * from (" + sql + ") tttable limit ?,?";
	}

	@Override
	protected Object[] getPageParameters() {
		return new Object[]{(currentPage - 1) * pageSize,pageSize};
	}
}
