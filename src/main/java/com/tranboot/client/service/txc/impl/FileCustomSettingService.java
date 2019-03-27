package com.tranboot.client.service.txc.impl;

import java.util.ArrayList;
import java.util.List;

import com.tranboot.client.model.txc.CustomSetting;
import com.tranboot.client.service.txc.TxcCustomSettingService;

public class FileCustomSettingService implements TxcCustomSettingService {
	
	private List<CustomSetting> settings = new ArrayList<>();
	
	public FileCustomSettingService(String xmlFile) {
		
	}

	@Override
	public boolean customField(String table, String field, Integer type) {
		return settings.contains(new CustomSetting(field, table, type));
	}

}
