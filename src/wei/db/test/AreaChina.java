package wei.db.test;

import wei.db.annotation.TableColumnAnnotation;
import wei.db.annotation.TableNameAnnotation;
import wei.db.annotation.TablePrimaryKeyAnnotation;

@TableNameAnnotation(name = "areachina", autoMap = true)
public class AreaChina {

	@TablePrimaryKeyAnnotation
	@TableColumnAnnotation
	private int areaCode;

	@TableColumnAnnotation
	private String areaName;

	@TableColumnAnnotation
	private int areaCodeDeprecated;

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
}
