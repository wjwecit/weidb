/**
 * 
 */
package wei.db.test;

import org.apache.commons.beanutils.BeanUtils;

import wei.db.common.Session;

/**
 * @author Qin-Wei
 * 
 */
public class MainTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Session session = new Session();
		AreaChina local = new AreaChina();
		local.setAreaCode(990);
		//local.setAreaCodeDeprecated(991);
		local.setAreaName("лан╡");

		try {
			//String p1 = BeanUtils.getSimpleProperty(local, "areaCode");
			//System.out.println(p1);

			session.executeUpdate("delete from areachina where areacode=?", new Object[] {990});
			if (session.insert(local)) {
				session.getMap("select * from areachina where areacode=990");
				session.getBeanList(AreaChina.class, "select * from areachina limit 5");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
