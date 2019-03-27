package com.tranboot.client.model.txc;

public class CustomSetting {
	public final String field;
	public final String table;
	public final Integer type;
	
	public CustomSetting (String field,String table,Integer type) {
		this.field = field;
		this.table = table;
		this.type = type;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof CustomSetting)) {
			return false;
		}
		CustomSetting that = (CustomSetting) obj;
        return (field.equals(that.field) && table.equals(that.table) && type.intValue() == that.type.intValue());
    }
}
