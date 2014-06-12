package wei.db.test;

import com.google.gson.Gson;

import wei.db.common.Table;

@Table(name = "areachina", key = "id")
public class AreaChina {

	private int areaCode;

	private String areaName;

	private int areaCodeDeprecated;
	
	private static Gson gson=new Gson();

	public int getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(int areaCode) {
		this.areaCode = areaCode;
	}

	public int getAreaCodeDeprecated() {
		return areaCodeDeprecated;
	}

	public void setAreaCodeDeprecated(int areaCodeDeprecated) {
		this.areaCodeDeprecated = areaCodeDeprecated;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}
	
	public String toString(){
		return gson.toJson(this);
	}
}
