/**
 * 
 */
package wei.db.test;

import java.util.List;

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
		// local.setAreaCodeDeprecated(991);
		local.setAreaName("лан╡");

		try {
			session.executeUpdate("delete from areachina where areacode=?", new Object[] { 990 });
			if (session.insert(local)) {
				session.getMap("select * from areachina where areacode=990");
				List<AreaChina> beanList = session.getBeanList(AreaChina.class,
						"select * from areachina where areacode like '110%'");
				for (AreaChina areaChina : beanList) {
					System.out.println(areaChina);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
