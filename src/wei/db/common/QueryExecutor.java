/**
 * 
 */
package wei.db.common;

import java.sql.ResultSet;

/**
 * ��ѯִ����,�������ڲ�ѯ����Ӧ��.
 * 
 * @author wangjunwei 2014-06-05
 * 
 */
public interface QueryExecutor {

	/**
	 * �����ѯ��Ľ����
	 */
	public void result(ResultSet rs) throws Exception;

	/**
	 * ���������������쳣ʱ,�÷���������,ͨ��������Ļع�����
	 */
	public void exception() throws Exception;

	/**
	 * �����������֮��,finally������. ͨ��ʱ����Դ�Ĺرղ���.
	 */
	public void after();
}
