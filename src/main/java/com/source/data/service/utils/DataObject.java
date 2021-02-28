package com.source.data.service.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private boolean valid = false;
 	
	private String json = "";
	
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	public String getJson() {
		return json;
	}
	public void setJson(String json) {
		this.json = json;
	}
	
	public String getString() {
		return "Json "+ this.json ;
	}
}
